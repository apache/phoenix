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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
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
    protected Table getHTableForSystemCatalog() throws SQLException {
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, configuration);
        return connection.getQueryServices().getTable(physicalTableName.getName());
    }

    @Override
    public InternalScanner createCompactionScanner(Store store,
                                                   InternalScanner s) {
        throw new UnsupportedOperationException();
    }

}