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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.RepairUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class IndexHalfStoreFileReaderGenerator extends BaseRegionObserver {

    private static final String LOCAL_INDEX_AUTOMATIC_REPAIR = "local.index.automatic.repair";
    public static final Logger LOGGER =
            LoggerFactory.getLogger(IndexHalfStoreFileReaderGenerator.class);

    @Override
    public Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
            FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
            Reference r, Reader reader) throws IOException {
        TableName tableName = ctx.getEnvironment().getRegion().getTableDesc().getTableName();
        Region region = ctx.getEnvironment().getRegion();
        HRegionInfo childRegion = region.getRegionInfo();
        byte[] splitKey = null;
        if (reader == null && r != null) {
            if(!p.toString().contains(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                return super.preStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, reader);
            }
            Scan scan = MetaTableAccessor.getScanForTableName(tableName);
            SingleColumnValueFilter scvf = null;
            if (Reference.isTopFileRegion(r.getFileRegion())) {
                scvf = new SingleColumnValueFilter(HConstants.CATALOG_FAMILY,
                        HConstants.SPLITB_QUALIFIER, CompareOp.EQUAL, region.getRegionInfo().toByteArray());
                scvf.setFilterIfMissing(true);
            } else {
                scvf = new SingleColumnValueFilter(HConstants.CATALOG_FAMILY,
                        HConstants.SPLITA_QUALIFIER, CompareOp.EQUAL, region.getRegionInfo().toByteArray());
                scvf.setFilterIfMissing(true);
            }
            if(scvf != null) scan.setFilter(scvf);
            byte[] regionStartKeyInHFile = null;
            HTable metaTable = null;
            PhoenixConnection conn = null;
            try {
                metaTable = new HTable(ctx.getEnvironment().getConfiguration(), TableName.META_TABLE_NAME);
                ResultScanner scanner = null;
                Result result = null;
                try {
                    scanner = metaTable.getScanner(scan);
                    result = scanner.next();
                } finally {
                    if(scanner != null) scanner.close();
                }
                if (result == null || result.isEmpty()) {
                    Pair<HRegionInfo, HRegionInfo> mergeRegions =
                            MetaTableAccessor.getRegionsFromMergeQualifier(ctx.getEnvironment()
                                    .getRegionServerServices().getConnection(),
                                region.getRegionInfo().getRegionName());
                    if (mergeRegions == null || mergeRegions.getFirst() == null) return reader;
                    byte[] splitRow =
                            CellUtil.cloneRow(KeyValue.createKeyValueFromKey(r.getSplitKey()));
                    // We need not change any thing in first region data because first region start key
                    // is equal to merged region start key. So returning same reader.
                    if (Bytes.compareTo(mergeRegions.getFirst().getStartKey(), splitRow) == 0) {
                        if (mergeRegions.getFirst().getStartKey().length == 0
                                && region.getRegionInfo().getEndKey().length != mergeRegions
                                        .getFirst().getEndKey().length) {
                            childRegion = mergeRegions.getFirst();
                            regionStartKeyInHFile =
                                    mergeRegions.getFirst().getStartKey().length == 0 ? new byte[mergeRegions
                                            .getFirst().getEndKey().length] : mergeRegions.getFirst()
                                            .getStartKey();
                        } else {
                            return reader;
                        }
                    } else {
                        childRegion = mergeRegions.getSecond();
                        regionStartKeyInHFile = mergeRegions.getSecond().getStartKey();
                    }
                    splitKey = KeyValue.createFirstOnRow(region.getRegionInfo().getStartKey().length == 0 ?
                        new byte[region.getRegionInfo().getEndKey().length] :
                            region.getRegionInfo().getStartKey()).getKey();
                } else {
                    HRegionInfo parentRegion = HRegionInfo.getHRegionInfo(result);
                    regionStartKeyInHFile =
                            parentRegion.getStartKey().length == 0 ? new byte[parentRegion
                                    .getEndKey().length] : parentRegion.getStartKey();
                }
            } finally {
                if (metaTable != null) metaTable.close();
            }
            try {
                conn = QueryUtil.getConnectionOnServer(ctx.getEnvironment().getConfiguration()).unwrap(
                            PhoenixConnection.class);
                PTable dataTable =
                        IndexUtil.getPDataTable(conn, ctx.getEnvironment().getRegion()
                                .getTableDesc());
                List<PTable> indexes = dataTable.getIndexes();
                Map<ImmutableBytesWritable, IndexMaintainer> indexMaintainers =
                        new HashMap<ImmutableBytesWritable, IndexMaintainer>();
                for (PTable index : indexes) {
                    if (index.getIndexType() == IndexType.LOCAL) {
                        IndexMaintainer indexMaintainer = index.getIndexMaintainer(dataTable, conn);
                        indexMaintainers.put(new ImmutableBytesWritable(index.getviewIndexIdType().toBytes(index.getViewIndexId())),
                            indexMaintainer);
                    }
                }
                if (indexMaintainers.isEmpty()) return reader;
                byte[][] viewConstants = getViewConstants(dataTable);
                return new IndexHalfStoreFileReader(fs, p, cacheConf, in, size, r,
                        ctx.getEnvironment().getConfiguration(), indexMaintainers, viewConstants,
                        childRegion, regionStartKeyInHFile, splitKey, region.getRegionInfo());
            } catch (SQLException e) {
                throw new IOException(e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        throw new IOException(e);
                    }
                }
            }
        }
        return reader;
    }

    @Override
    public InternalScanner preCompact(
            ObserverContext<RegionCoprocessorEnvironment> c, Store store,
            InternalScanner s, ScanType scanType,
            CompactionRequest request) throws IOException {
        if (!IndexUtil.isLocalIndexStore(store)) { return s; }
        if (!store.hasReferences()) {
            InternalScanner repairScanner = null;
            if (request.isMajor() && (!RepairUtil.isLocalIndexStoreFilesConsistent(c.getEnvironment(), store))) {
                LOGGER.info("we have found inconsistent data for local index for region:"
                        + c.getEnvironment().getRegion().getRegionInfo());
                if (c.getEnvironment().getConfiguration().getBoolean(LOCAL_INDEX_AUTOMATIC_REPAIR, true)) {
                    LOGGER.info("Starting automatic repair of local Index for region:"
                            + c.getEnvironment().getRegion().getRegionInfo());
                    repairScanner = getRepairScanner(c.getEnvironment(), store);
                }
            }
            if (repairScanner != null) {
                if (s!=null) {
                    s.close();
                }
                return repairScanner;
            } else {
                return s;
            }
        }
       return s;
    }

    private byte[][] getViewConstants(PTable dataTable) {
        int dataPosOffset = (dataTable.getBucketNum() != null ? 1 : 0) + (dataTable.isMultiTenant() ? 1 : 0);
        byte[][] viewConstants = null;
        int nViewConstants = 0;
        if (dataTable.getType() == PTableType.VIEW) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            List<PColumn> dataPkColumns = dataTable.getPKColumns();
            for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
                PColumn dataPKColumn = dataPkColumns.get(i);
                if (dataPKColumn.getViewConstant() != null) {
                    nViewConstants++;
                }
            }
            if (nViewConstants > 0) {
                viewConstants = new byte[nViewConstants][];
                int j = 0;
                for (int i = dataPosOffset; i < dataPkColumns.size(); i++) {
                    PColumn dataPkColumn = dataPkColumns.get(i);
                    if (dataPkColumn.getViewConstant() != null) {
                        if (IndexUtil.getViewConstantValue(dataPkColumn, ptr)) {
                            viewConstants[j++] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                        } else {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
        }
        return viewConstants;
    }

    /**
     * @param env
     * @param store Local Index store
     * @return StoreScanner for new Local Index data for a passed store and Null if repair is not possible
     * @throws IOException
     */
    private InternalScanner getRepairScanner(RegionCoprocessorEnvironment env, Store store) throws IOException {
        //List<KeyValueScanner> scannersForStoreFiles = Lists.newArrayListWithExpectedSize(store.getStorefilesCount());
        Scan scan = new Scan();
        scan.setMaxVersions(store.getFamily().getMaxVersions());
        for (Store s : env.getRegion().getStores()) {
            if (!IndexUtil.isLocalIndexStore(s)) {
                scan.addFamily(s.getFamily().getName());
            }
        }
        try {
            PhoenixConnection conn = QueryUtil.getConnectionOnServer(env.getConfiguration())
                    .unwrap(PhoenixConnection.class);
            PTable dataPTable = IndexUtil.getPDataTable(conn, env.getRegion().getTableDesc());
            final List<IndexMaintainer> maintainers = Lists
                    .newArrayListWithExpectedSize(dataPTable.getIndexes().size());
            for (PTable index : dataPTable.getIndexes()) {
                if (index.getIndexType() == IndexType.LOCAL) {
                    maintainers.add(index.getIndexMaintainer(dataPTable, conn));
                }
            }
            return new DataTableLocalIndexRegionScanner(env.getRegion().getScanner(scan), env.getRegion(),
                    maintainers, store.getFamily().getName(),env.getConfiguration());


        } catch (SQLException e) {
            throw new IOException(e);

        }
    }

}
