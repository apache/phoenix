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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;

/**
 * Implementation for DefaultStatisticsCollector when running inside RegionServer process
 * Triggered via "UPDATE STATISTICS" SQL statement
 */
public class RegionServerStatisticsCollector extends DefaultStatisticsCollector {

    private static final Log LOG = LogFactory.getLog(RegionServerStatisticsCollector.class);
    final private RegionCoprocessorEnvironment env;

    RegionServerStatisticsCollector(RegionCoprocessorEnvironment env, String tableName,
                                    long clientTimeStamp, byte[] family, byte[] gp_width_bytes,
                                    byte[] gp_per_region_bytes) {
        super(env.getConfiguration(), env.getRegion(), tableName, clientTimeStamp, family, gp_width_bytes, gp_per_region_bytes);
        this.env = env;
    }

    @Override
    protected void initStatsWriter() throws IOException {
        this.statsWriter = StatisticsWriter.newWriter(env, tableName, clientTimeStamp, guidePostDepth);
    }

    @Override
    protected Table getHTableForSystemCatalog() throws IOException {
        return env.getTable(
                SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, env.getConfiguration()));
    }

    @Override
    public InternalScanner createCompactionScanner(Store store,
                                                   InternalScanner s) throws IOException {
        // See if this is for Major compaction
        if (LOG.isDebugEnabled()) {
            LOG.debug("Compaction scanner created for stats");
        }
        ImmutableBytesPtr cfKey = new ImmutableBytesPtr(store.getFamily().getName());
        // Potentially perform a cross region server get in order to use the correct guide posts
        // width for the table being compacted.
        init();
        StatisticsScanner scanner = new StatisticsScanner(this, statsWriter, env, s, cfKey);
        return scanner;
    }

}
