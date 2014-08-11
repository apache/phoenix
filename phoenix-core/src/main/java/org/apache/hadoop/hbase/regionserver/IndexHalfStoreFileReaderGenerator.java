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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
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
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

public class IndexHalfStoreFileReaderGenerator extends BaseRegionObserver {
    
    @Override
    public Reader preStoreFileReaderOpen(ObserverContext<RegionCoprocessorEnvironment> ctx,
            FileSystem fs, Path p, FSDataInputStreamWrapper in, long size, CacheConfig cacheConf,
            Reference r, Reader reader) throws IOException {
        TableName tableName = ctx.getEnvironment().getRegion().getTableDesc().getTableName();
        HRegion region = ctx.getEnvironment().getRegion();
        if (reader == null && r != null) {
            Scan scan = MetaReader.getScanForTableName(tableName);
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
            HRegionInfo parentRegion = null;
            HTable metaTable = null;
            PhoenixConnection conn = null;
            try {
                metaTable = new HTable(ctx.getEnvironment().getConfiguration(), TableName.META_TABLE_NAME);
                ResultScanner scanner = metaTable.getScanner(scan);
                Result result = scanner.next();
                if (result == null || result.isEmpty()) return reader;
                parentRegion = HRegionInfo.getHRegionInfo(result);
            } finally {
                if (metaTable != null) metaTable.close();
            }
            try {
                conn = QueryUtil.getConnection(ctx.getEnvironment().getConfiguration()).unwrap(
                            PhoenixConnection.class);
                String userTableName = MetaDataUtil.getUserTableName(tableName.getNameAsString());
                PTable dataTable = PhoenixRuntime.getTable(conn, userTableName);
                List<PTable> indexes = dataTable.getIndexes();
                Map<ImmutableBytesWritable, IndexMaintainer> indexMaintainers =
                        new HashMap<ImmutableBytesWritable, IndexMaintainer>();
                for (PTable index : indexes) {
                    if (index.getIndexType() == IndexType.LOCAL) {
                        IndexMaintainer indexMaintainer = index.getIndexMaintainer(dataTable);
                        indexMaintainers.put(new ImmutableBytesWritable(MetaDataUtil
                                .getViewIndexIdDataType().toBytes(index.getViewIndexId())),
                            indexMaintainer);
                    }
                }
                if(indexMaintainers.isEmpty()) return reader;
                byte[][] viewConstants = getViewConstants(dataTable);
                return new IndexHalfStoreFileReader(fs, p, cacheConf, in, size, r, ctx
                        .getEnvironment().getConfiguration(), indexMaintainers, viewConstants,
                        region.getRegionInfo(), parentRegion);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
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
}
