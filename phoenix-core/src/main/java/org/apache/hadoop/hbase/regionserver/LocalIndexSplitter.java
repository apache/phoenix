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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class LocalIndexSplitter extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(LocalIndexSplitter.class);

    private SplitTransaction st = null;
    private PairOfSameType<HRegion> daughterRegions = null;
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    private static final int SPLIT_TXN_MINIMUM_SUPPORTED_VERSION = VersionUtil
            .encodeVersion("0.98.9");

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx,
            byte[] splitKey, List<Mutation> metaEntries) throws IOException {
        RegionCoprocessorEnvironment environment = ctx.getEnvironment();
        HTableDescriptor tableDesc = ctx.getEnvironment().getRegion().getTableDesc();
        if (SchemaUtil.isSystemTable(tableDesc.getName())) {
            return;
        }
        RegionServerServices rss = ctx.getEnvironment().getRegionServerServices();
        if (tableDesc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) == null
                || !Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(tableDesc
                        .getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
            TableName indexTable =
                    TableName.valueOf(MetaDataUtil.getLocalIndexPhysicalName(tableDesc.getName()));
            if (!MetaTableAccessor.tableExists(rss.getConnection(), indexTable)) return;

            HRegion indexRegion = IndexUtil.getIndexRegion(environment);
            if (indexRegion == null) {
                LOG.warn("Index region corresponindg to data region " + environment.getRegion()
                        + " not in the same server. So skipping the split.");
                ctx.bypass();
                return;
            }
            try {
                int encodedVersion = VersionUtil.encodeVersion(environment.getHBaseVersion());
                if(encodedVersion >= SPLIT_TXN_MINIMUM_SUPPORTED_VERSION) {
                    st = new SplitTransaction(indexRegion, splitKey);
                    st.useZKForAssignment =
                            environment.getConfiguration().getBoolean("hbase.assignment.usezk",
                                true);
                } else {
                    st = new IndexSplitTransaction(indexRegion, splitKey);
                }

                if (!st.prepare()) {
                    LOG.error("Prepare for the table " + indexRegion.getTableDesc().getNameAsString()
                        + " failed. So returning null. ");
                    ctx.bypass();
                    return;
                }
                indexRegion.forceSplit(splitKey);
                daughterRegions = st.stepsBeforePONR(rss, rss, false);
                HRegionInfo copyOfParent = new HRegionInfo(indexRegion.getRegionInfo());
                copyOfParent.setOffline(true);
                copyOfParent.setSplit(true);
                // Put for parent
                Put putParent = MetaTableAccessor.makePutFromRegionInfo(copyOfParent);
                MetaTableAccessor.addDaughtersToPut(putParent,
                        daughterRegions.getFirst().getRegionInfo(),
                        daughterRegions.getSecond().getRegionInfo());
                metaEntries.add(putParent);
                // Puts for daughters
                Put putA = MetaTableAccessor.makePutFromRegionInfo(
                        daughterRegions.getFirst().getRegionInfo());
                Put putB = MetaTableAccessor.makePutFromRegionInfo(
                        daughterRegions.getSecond().getRegionInfo());
                st.addLocation(putA, rss.getServerName(), 1);
                st.addLocation(putB, rss.getServerName(), 1);
                metaEntries.add(putA);
                metaEntries.add(putB);
            } catch (Exception e) {
                ctx.bypass();
                LOG.warn("index region splitting failed with the exception ", e);
                if (st != null){
                    st.rollback(rss, rss);
                    st = null;
                    daughterRegions = null;
                }
            }
        }
    }

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        if (st == null || daughterRegions == null) return;
        RegionCoprocessorEnvironment environment = ctx.getEnvironment();
        PhoenixConnection conn = null;
        try {
            conn = QueryUtil.getConnection(ctx.getEnvironment().getConfiguration()).unwrap(
                PhoenixConnection.class);
            MetaDataClient client = new MetaDataClient(conn);
            String userTableName = ctx.getEnvironment().getRegion().getTableDesc().getNameAsString();
            PTable dataTable = PhoenixRuntime.getTable(conn, userTableName);
            List<PTable> indexes = dataTable.getIndexes();
            for (PTable index : indexes) {
                if (index.getIndexType() == IndexType.LOCAL) {
                    AlterIndexStatement indexStatement = FACTORY.alterIndex(FACTORY.namedTable(null,
                        org.apache.phoenix.parse.TableName.create(index.getSchemaName().getString(), index.getTableName().getString())),
                        dataTable.getTableName().getString(), false, PIndexState.INACTIVE);
                    client.alterIndex(indexStatement);
                }
            }
            conn.commit();
        } catch (ClassNotFoundException ex) {
        } catch (SQLException ex) {
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                }
            }
        }

        HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
        st.stepsAfterPONR(rs, rs, daughterRegions);
    }
    
    @Override
    public void preRollBackSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        RegionCoprocessorEnvironment environment = ctx.getEnvironment();
        HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
        try {
            if (st != null) {
                st.rollback(rs, rs);
                st = null;
                daughterRegions = null;
            }
        } catch (Exception e) {
            if (st != null) {
                LOG.error("Error while rolling back the split failure for index region", e);
            }
            rs.abort("Abort; we got an error during rollback of index");
        }
    }

}
