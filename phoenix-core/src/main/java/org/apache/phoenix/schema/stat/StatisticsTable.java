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
package org.apache.phoenix.schema.stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Closeable;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.TimeKeeper;

/**
 * Wrapper to access the statistics table SYSTEM.STATS using the HTable.
 */
public class StatisticsTable implements Closeable {

    private static final Log LOG = LogFactory.getLog(StatisticsTable.class);
    /** Map of the currently open statistics tables */
    private static final Map<String, StatisticsTable> tableMap = new HashMap<String, StatisticsTable>();
    private TimeKeeper timeKeeper = TimeKeeper.SYSTEM;

    /**
     * @param env
     *            Environment wherein the coprocessor is attempting to update the stats table.
     * @param primaryTableName
     *            name of the primary table on which we should collect stats
     * @return the {@link StatisticsTable} for the given primary table.
     * @throws IOException
     *             if the table cannot be created due to an underlying HTable creation error
     */
    public synchronized static StatisticsTable getStatisticsTableForCoprocessor(CoprocessorEnvironment env,
            byte[] primaryTableName) throws IOException {
        StatisticsTable table = tableMap.get(primaryTableName);
        if (table == null) {
            // Map the statics table and the table with which the statistics is
            // associated
            table = new StatisticsTable(
                    env.getTable(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES)), primaryTableName);
            tableMap.put(Bytes.toString(primaryTableName), table);
        }
        return table;
    }

    private final HTableInterface statisticsTable;
    private final byte[] sourceTableName;

    private StatisticsTable(HTableInterface statsTable, byte[] sourceTableName) {
        this.statisticsTable = statsTable;
        this.sourceTableName = sourceTableName;
    }

    public StatisticsTable(Configuration conf, HTableDescriptor source) throws IOException {
        this(new HTable(conf, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME), source.getName());
    }

    /**
     * Close the connection to the table
     */
    @Override
    public void close() throws IOException {
        statisticsTable.close();
    }

    /**
     * Update a list of statistics for the given region
     * 
     * @param serializer
     *            to convert the actual statistics to puts in the statistics table
     * @param tracker
     * @param fam
     * @param fullTableUpdate
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the remaining list of stats to
     *             update
     */
    public void updateStats(byte[] tableKey, byte[] regionName, StatisticsTracker tracker, byte[] fam,
            boolean fullTableUpdate) throws IOException {
        // short circuit if we have nothing to write
        if (tracker == null) { return; }

        byte[] prefix = StatisticsUtils.getRowKey(tableKey, fam, regionName);
        Put put = new Put(prefix);
        if (fullTableUpdate) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_IN_MS_BYTES,
                    Bytes.toBytes(timeKeeper.getCurrentTime()));
        }
        // TODO : Use Phoenix-1101 and use upsert stmt to insert into the SYSTEM.STATS table
        if (tracker.getGuidePosts(fam) != null) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES,
                    (tracker.getGuidePosts(fam)));
        }
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
                PDataType.VARBINARY.toBytes(tracker.getMinKey(fam)));
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
                PDataType.VARBINARY.toBytes(tracker.getMaxKey(fam)));
        // serialize each of the metrics with the associated serializer
        statisticsTable.put(put);

        statisticsTable.flushCommits();
    }

    /**
     * @return the underlying {@link HTableInterface} to which this table is writing
     */
    HTableInterface getUnderlyingTable() {
        return statisticsTable;
    }

    byte[] getSourceTableName() {
        return this.sourceTableName;
    }
}