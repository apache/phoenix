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
package org.apache.phoenix.schema.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation for DefaultStatisticsCollector when running inside Hadoop MR job mapper
 * Triggered via UpdateStatisticsTool class
 */
public class MapperStatisticsCollector extends DefaultStatisticsCollector {

    private PhoenixConnection connection;

    public MapperStatisticsCollector(PhoenixConnection connection, Configuration conf, Region region, String tableName,
                                     long clientTimeStamp, byte[] family, byte[] gp_width_bytes,
                                     byte[] gp_per_region_bytes) {
        super(conf, region, tableName,
                clientTimeStamp, family, gp_width_bytes, gp_per_region_bytes);
        this.connection = connection;
    }

    @Override
    protected void initStatsWriter() throws IOException, SQLException {
        this.statsWriter = StatisticsWriter.newWriter(connection, tableName, clientTimeStamp, guidePostDepth);
    }

    @Override
    protected long getGuidePostDepthFromSystemCatalog() throws IOException, SQLException {
        long guidepostWidth = -1;
        Table htable = null;
        try {
            TableName physicalTableName = SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, configuration);
            // Next check for GUIDE_POST_WIDTH on table
            htable = connection.getQueryServices().getTable(physicalTableName.getName());
            Get get = new Get(ptableKey);
            get.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
            Result result = htable.get(get);
            if (!result.isEmpty()) {
                Cell cell = result.listCells().get(0);
                guidepostWidth = PLong.INSTANCE.getCodec().decodeLong(cell.getValueArray(), cell.getValueOffset(), SortOrder.getDefault());
            } else if (!isViewIndexTable) {
                /*
                 * The table we are collecting stats for is potentially a base table, or local
                 * index or a global index. For view indexes, we rely on the the guide post
                 * width column in the parent data table's metadata which we already tried
                 * retrieving above.
                 */
                try (Connection conn =
                             QueryUtil.getConnectionOnServer(this.configuration)) {
                    PTable table = PhoenixRuntime.getTable(conn, tableName);
                    if (table.getType() == PTableType.INDEX
                            && table.getIndexType() == PTable.IndexType.GLOBAL) {
                        /*
                         * For global indexes, we need to get the parentName first and then
                         * fetch guide post width configured for the parent table.
                         */
                        PName parentName = table.getParentName();
                        byte[] parentKey =
                                SchemaUtil.getTableKeyFromFullName(parentName.getString());
                        get = new Get(parentKey);
                        get.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH_BYTES);
                        result = htable.get(get);
                        if (!result.isEmpty()) {
                            Cell cell = result.listCells().get(0);
                            guidepostWidth =
                                    PLong.INSTANCE.getCodec().decodeLong(cell.getValueArray(),
                                            cell.getValueOffset(), SortOrder.getDefault());
                        }
                    }
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close " + htable.getName(), e);
                }
            }
        }
        return guidepostWidth;
    }

    @Override
    public InternalScanner createCompactionScanner(RegionCoprocessorEnvironment env, Store store,
                                                   InternalScanner s) {
        throw new UnsupportedOperationException();
    }

}