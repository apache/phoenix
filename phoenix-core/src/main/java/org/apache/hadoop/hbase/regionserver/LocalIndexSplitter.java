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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.IndexSplitTransaction;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

public class LocalIndexSplitter extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(LocalIndexSplitter.class);

    private IndexSplitTransaction st = null;
    private PairOfSameType<HRegion> daughterRegions = null;

    @Override
    public void preSplitBeforePONR(ObserverContext<RegionCoprocessorEnvironment> ctx,
            byte[] splitKey, List<Mutation> metaEntries) throws IOException {
        RegionCoprocessorEnvironment environment = ctx.getEnvironment();
        HTableDescriptor tableDesc = ctx.getEnvironment().getRegion().getTableDesc();
        if (SchemaUtil.isMetaTable(tableDesc.getName())
                || SchemaUtil.isSequenceTable(tableDesc.getName())) {
            return;
        }
        RegionServerServices rss = ctx.getEnvironment().getRegionServerServices();
        if (tableDesc.getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES) == null
                || !Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(tableDesc
                        .getValue(MetaDataUtil.IS_LOCAL_INDEX_TABLE_PROP_BYTES)))) {
            HRegion indexRegion = IndexUtil.getIndexRegion(environment);
            if (indexRegion == null) return;
            st = new IndexSplitTransaction(indexRegion, splitKey);
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
            Put putParent = MetaEditor.makePutFromRegionInfo(copyOfParent);
            MetaEditor.addDaughtersToPut(putParent, daughterRegions.getFirst().getRegionInfo(),
                daughterRegions.getSecond().getRegionInfo());
            metaEntries.add(putParent);
            // Puts for daughters
            Put putA = MetaEditor.makePutFromRegionInfo(daughterRegions.getFirst().getRegionInfo());
            Put putB =
                    MetaEditor.makePutFromRegionInfo(daughterRegions.getSecond().getRegionInfo());
            st.addLocation(putA, rss.getServerName(), 1);
            st.addLocation(putB, rss.getServerName(), 1);
            metaEntries.add(putA);
            metaEntries.add(putB);
        }
    }

    @Override
    public void preSplitAfterPONR(ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
        if (st == null || daughterRegions == null) return;
        RegionCoprocessorEnvironment environment = ctx.getEnvironment();
        HRegionServer rs = (HRegionServer) environment.getRegionServerServices();
        st.stepsAfterPONR(rs, rs, daughterRegions);
    }
}
