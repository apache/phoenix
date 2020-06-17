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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.*;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getCurrentScnValue;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getDisableLoggingVerifyType;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolDataTableName;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolIndexTableName;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolLastVerifyTime;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexVerifyType;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolStartTime;
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

        try (final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps)) {
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            Long scn = (currentScnValue != null) ? Long.valueOf(currentScnValue) : EnvironmentEdgeManager.currentTimeMillis();
            setCurrentScnValue(configuration, scn);

            Long startTime = (startTimeValue == null) ? 0L : Long.valueOf(startTimeValue);
            PTable indexTable = PhoenixRuntime.getTableNoCache(phoenixConnection, indexTableFullName);
            ServerBuildIndexCompiler compiler =
                    new ServerBuildIndexCompiler(phoenixConnection, dataTableFullName);
            MutationPlan plan = compiler.compile(indexTable);
            Scan scan = plan.getContext().getScan();
            Long lastVerifyTimeValue = lastVerifyTime == null ? 0L : Long.valueOf(lastVerifyTime);
            try {
                scan.setTimeRange(startTime, scn);
                scan.setAttribute(BaseScannerRegionObserver.INDEX_REBUILD_PAGING, TRUE_BYTES);
                scan.setAttribute(BaseScannerRegionObserver.INDEX_REBUILD_VERIFY_TYPE, getIndexVerifyType(configuration).toBytes());
                scan.setAttribute(BaseScannerRegionObserver.INDEX_RETRY_VERIFY, Bytes.toBytes(lastVerifyTimeValue));
                scan.setAttribute(BaseScannerRegionObserver.INDEX_REBUILD_DISABLE_LOGGING_VERIFY_TYPE,
                    getDisableLoggingVerifyType(configuration).toBytes());
            } catch (IOException e) {
                throw new SQLException(e);
            }
            queryPlan = plan.getQueryPlan();
            // since we can't set a scn on connections with txn set TX_SCN attribute so that the max time range is set by BaseScannerRegionObserver
            if (txnScnValue != null) {
                scan.setAttribute(BaseScannerRegionObserver.TX_SCN, Bytes.toBytes(Long.valueOf(txnScnValue)));
            }
            return queryPlan;
        } catch (Exception exception) {
            LOGGER.error(String.format("Failed to get the query plan with error [%s]",
                    exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }
}
