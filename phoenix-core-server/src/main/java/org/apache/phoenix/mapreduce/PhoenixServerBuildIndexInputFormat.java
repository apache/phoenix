/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ServerBuildIndexCompiler;
import org.apache.phoenix.compile.ServerBuildTransformingTableCompiler;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getCurrentScnValue;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getDisableLoggingVerifyType;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolDataTableName;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolIndexTableName;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolLastVerifyTime;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolSourceTable;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexVerifyType;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolStartTime;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIsTransforming;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.setCurrentScnValue;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

/**
 * {@link InputFormat} implementation from Phoenix for building index
 * 
 */
public class PhoenixServerBuildIndexInputFormat<T extends DBWritable> extends PhoenixInputFormat {
    QueryPlan queryPlan = null;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixServerBuildIndexInputFormat.class);

    /**
     * instantiated by framework
     */
    public PhoenixServerBuildIndexInputFormat() {
    }

    private interface QueryPlanBuilder {
        QueryPlan getQueryPlan(PhoenixConnection phoenixConnection, String dataTableFullName,
            String indexTableFullName) throws SQLException;
    }

    private class TransformingDataTableQueryPlanBuilder implements QueryPlanBuilder {
        @Override
        public QueryPlan getQueryPlan(PhoenixConnection phoenixConnection, String oldTableFullName,
                                      String newTableFullName) throws SQLException {
            PTable newTable = phoenixConnection.getTableNoCache(newTableFullName);
            ServerBuildTransformingTableCompiler compiler = new ServerBuildTransformingTableCompiler(phoenixConnection, oldTableFullName);
            MutationPlan plan = compiler.compile(newTable);
            return plan.getQueryPlan();
        }
    }

    private class DataTableQueryPlanBuilder implements QueryPlanBuilder {
        @Override
        public QueryPlan getQueryPlan(PhoenixConnection phoenixConnection, String dataTableFullName,
            String indexTableFullName) throws SQLException {
            PTable indexTable = phoenixConnection.getTableNoCache(indexTableFullName);
            ServerBuildIndexCompiler compiler = new ServerBuildIndexCompiler(phoenixConnection, dataTableFullName);
            MutationPlan plan = compiler.compile(indexTable);
            return plan.getQueryPlan();
        }
    }

    private class IndexTableQueryPlanBuilder implements QueryPlanBuilder {
        @Override
        public QueryPlan getQueryPlan(PhoenixConnection phoenixConnection, String dataTableFullName,
            String indexTableFullName) throws SQLException {
            QueryPlan plan;
            try (final PhoenixStatement statement = new PhoenixStatement(phoenixConnection)) {
                String query = "SELECT count(*) FROM " + indexTableFullName;
                plan = statement.compileQuery(query);
                TableRef tableRef = plan.getTableRef();
                Scan scan = plan.getContext().getScan();
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                PTable pIndexTable = tableRef.getTable();
                PTable pDataTable = phoenixConnection.getTable(dataTableFullName);
                IndexMaintainer.serialize(pDataTable, ptr, Collections.singletonList(pIndexTable), phoenixConnection);
                scan.setAttribute(PhoenixIndexCodec.INDEX_NAME_FOR_IDX_MAINTAINER,
                        pIndexTable.getTableName().getBytes());
                ScanUtil.annotateScanWithMetadataAttributes(pDataTable, scan);
                scan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD,
                        ByteUtil.copyKeyBytesIfNecessary(ptr));
                scan.setAttribute(BaseScannerRegionObserverConstants.REBUILD_INDEXES, TRUE_BYTES);
                ScanUtil.setClientVersion(scan, MetaDataProtocol.PHOENIX_VERSION);
            }
            return plan;
        }
    }

    private QueryPlanBuilder queryPlanBuilder;

    @Override
    protected QueryPlan getQueryPlan(final JobContext context, final Configuration configuration)
            throws IOException {
        Preconditions.checkNotNull(context);
        if (queryPlan != null) {
            return queryPlan;
        }
        final String txnScnValue = configuration.get(PhoenixConfigurationUtil.TX_SCN_VALUE);
        final String currentScnValue = getCurrentScnValue(configuration);
        final String startTimeValue = getIndexToolStartTime(configuration);
        final String tenantId = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);
        final String lastVerifyTime = getIndexToolLastVerifyTime(configuration);

        final Properties overridingProps = new Properties();
        if (txnScnValue==null && currentScnValue!=null) {
            overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
        }
        if (tenantId != null && configuration.get(PhoenixRuntime.TENANT_ID_ATTRIB) == null) {
            overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        String dataTableFullName = getIndexToolDataTableName(configuration);
        String indexTableFullName = getIndexToolIndexTableName(configuration);
        SourceTable sourceTable = getIndexToolSourceTable(configuration);
        if (getIsTransforming(configuration) &&
                PhoenixConfigurationUtil.getTransformingTableType(configuration) == SourceTable.DATA_TABLE_SOURCE) {
            queryPlanBuilder = new TransformingDataTableQueryPlanBuilder();
        } else {
            queryPlanBuilder = sourceTable.equals(SourceTable.DATA_TABLE_SOURCE) ?
                    new DataTableQueryPlanBuilder() : new IndexTableQueryPlanBuilder();
        }

        try (final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps)) {
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            Long scn = (currentScnValue != null) ? Long.parseLong(currentScnValue) : EnvironmentEdgeManager.currentTimeMillis();
            setCurrentScnValue(configuration, scn);

            Long startTime = (startTimeValue == null) ? 0L : Long.parseLong(startTimeValue);

            queryPlan = queryPlanBuilder.getQueryPlan(phoenixConnection, dataTableFullName, indexTableFullName);
            Scan scan = queryPlan.getContext().getScan();

            Long lastVerifyTimeValue = lastVerifyTime == null ? 0L : Long.parseLong(lastVerifyTime);
            try {
                scan.setTimeRange(startTime, scn);
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGING, TRUE_BYTES);
                // Serialize page row size only if we're overriding, else use server side value
                String rebuildPageRowSize =
                        configuration.get(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS);
                if (rebuildPageRowSize != null) {
                    scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_PAGE_ROWS,
                        Bytes.toBytes(Long.parseLong(rebuildPageRowSize)));
                }
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_VERIFY_TYPE, getIndexVerifyType(configuration).toBytes());
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_RETRY_VERIFY, Bytes.toBytes(lastVerifyTimeValue));
                scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_DISABLE_LOGGING_VERIFY_TYPE,
                    getDisableLoggingVerifyType(configuration).toBytes());
                String shouldLogMaxLookbackOutput =
                    configuration.get(IndexRebuildRegionScanner.PHOENIX_INDEX_MR_LOG_BEYOND_MAX_LOOKBACK_ERRORS);
                if (shouldLogMaxLookbackOutput != null) {
                    scan.setAttribute(BaseScannerRegionObserverConstants.INDEX_REBUILD_DISABLE_LOGGING_BEYOND_MAXLOOKBACK_AGE,
                        Bytes.toBytes(shouldLogMaxLookbackOutput));
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
            // since we can't set a scn on connections with txn set TX_SCN attribute so that the max time range is set by BaseScannerRegionObserver
            if (txnScnValue != null) {
                scan.setAttribute(BaseScannerRegionObserverConstants.TX_SCN, Bytes.toBytes(Long.parseLong(txnScnValue)));
            }
            return queryPlan;
        } catch (Exception exception) {
            LOGGER.error(String.format("Failed to get the query plan with error [%s]",
                    exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }
}
