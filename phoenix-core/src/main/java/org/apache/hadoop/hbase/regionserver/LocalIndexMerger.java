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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

public class LocalIndexMerger extends BaseRegionServerObserver {

    private static final Log LOG = LogFactory.getLog(LocalIndexMerger.class);

    private RegionMergeTransaction rmt = null;
    private HRegion mergedRegion = null;

    @Override
    public void preMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
            HRegion regionA, HRegion regionB, List<Mutation> metaEntries) throws IOException {
        HTableDescriptor tableDesc = regionA.getTableDesc();
        if (SchemaUtil.isSystemTable(tableDesc.getName())) {
            return;
        }
        RegionServerServices rss = ctx.getEnvironment().getRegionServerServices();
        HRegionServer rs = (HRegionServer) rss;
        if (tableDesc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) == null
                || !Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(tableDesc
                        .getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
            TableName indexTable =
                    TableName.valueOf(MetaDataUtil.getLocalIndexPhysicalName(tableDesc.getName()));
            if (!MetaTableAccessor.tableExists(rs.getConnection(), indexTable)) return;
            HRegion indexRegionA = IndexUtil.getIndexRegion(regionA, ctx.getEnvironment());
            if (indexRegionA == null) {
                LOG.warn("Index region corresponindg to data region " + regionA
                        + " not in the same server. So skipping the merge.");
                ctx.bypass();
                return;
            }
            HRegion indexRegionB = IndexUtil.getIndexRegion(regionB, ctx.getEnvironment());
            if (indexRegionB == null) {
                LOG.warn("Index region corresponindg to region " + regionB
                        + " not in the same server. So skipping the merge.");
                ctx.bypass();
                return;
            }
            try {
                rmt = new RegionMergeTransaction(indexRegionA, indexRegionB, false);
                if (!rmt.prepare(rss)) {
                    LOG.error("Prepare for the index regions merge [" + indexRegionA + ","
                            + indexRegionB + "] failed. So returning null. ");
                    ctx.bypass();
                    return;
                }
                this.mergedRegion = rmt.stepsBeforePONR(rss, rss, false);
                rmt.prepareMutationsForMerge(mergedRegion.getRegionInfo(),
                    indexRegionA.getRegionInfo(), indexRegionB.getRegionInfo(),
                    rss.getServerName(), metaEntries);
            } catch (Exception e) {
                ctx.bypass();
                LOG.warn("index regions merge failed with the exception ", e);
                if (rmt != null) {
                    rmt.rollback(rss, rss);
                    rmt = null;
                    mergedRegion = null;
                }
            }
        }
    }

    @Override
    public void postMergeCommit(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
            HRegion regionA, HRegion regionB, HRegion mergedRegion) throws IOException {
        if (rmt != null && this.mergedRegion != null) {
            RegionServerCoprocessorEnvironment environment = ctx.getEnvironment();
            HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
            rmt.stepsAfterPONR(rs, rs, this.mergedRegion);
        }
    }

    @Override
    public void preRollBackMerge(ObserverContext<RegionServerCoprocessorEnvironment> ctx,
            HRegion regionA, HRegion regionB) throws IOException {
        HRegionServer rs = (HRegionServer) ctx.getEnvironment().getRegionServerServices();
        try {
            if (rmt != null) {
                rmt.rollback(rs, rs);
                rmt = null;
                mergedRegion = null;
            }
        } catch (Exception e) {
            LOG.error("Error while rolling back the merge failure for index regions", e);
            rs.abort("Abort; we got an error during rollback of index");
        }
    }
}
