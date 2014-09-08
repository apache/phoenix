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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_IN_MS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REGION_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    private static final String UPDATE_STATS =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_STATS_TABLE + "\"( " + 
            TABLE_NAME + "," +
            COLUMN_NAME + "," +
            REGION_NAME + "," +
            LAST_STATS_UPDATE_TIME_IN_MS + "," +
            MIN_KEY + "," +
            MAX_KEY + "," +
            GUIDE_POSTS  +
            ") VALUES (?, ?, ?, ?, ?, ? , ?)";
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
     * Update a list of statistics for a given region.  If the ANALYZE <tablename> query is issued
     * then we use Upsert queries to update the table
     * If the region gets splitted or the major compaction happens we update using HTable.put()
     * @param tablekey
     *            The table name formed by combining the tenantid, schemaid and table name
     * @param region name -  the region of the table for which the stats are collected
     * @param tracker - the statistics tracker
     * @param fam -  the family for which the stats is getting collected.
     * @param url - the connection url - not null if coming from ANALYZE <tablename> query, else null
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the remaining list of stats to
     *             update
     */
    public void updateStats(String tableKey, String regionName, StatisticsTracker tracker, String fam,
            String url) throws IOException {
        if (tracker == null) { return; }

        long currentTime = timeKeeper.getCurrentTime();
        if (url != null) {
            // If url is not null then the update has come from the ANALYZE query
            try {
                // For every update create a new connection and call update stats
                Connection connection = DriverManager.getConnection(url);
                try {
                    PreparedStatement stmt = connection.prepareStatement(UPDATE_STATS);
                    stmt.setString(1, tableKey);
                    stmt.setString(2, fam);
                    stmt.setString(3, regionName);
                    stmt.setLong(4, currentTime);
                    stmt.setBytes(5, tracker.getMinKey(fam));
                    stmt.setBytes(6, tracker.getMaxKey(fam));
                    stmt.setBytes(7, tracker.getGuidePosts(fam));
                    stmt.execute();

                    connection.commit();
                } finally {
                    connection.close();
                }
            } catch (SQLException e) {
                LOG.error("Failed to update the stats table ", e);
                throw new IOException(e);
            }
        } else {
            // The format we try to write here is the same format as we try to write the statistics using UPSERT stmt
            // This gets called during major compaction and region split and we don't write any last_update_stats here.
            // Only when Analyze stats is called we do that.
            List<Put> puts = new ArrayList<Put>();
            byte[] prefix = StatisticsUtils.getRowKey(PDataType.VARCHAR.toBytes(tableKey),
                    PDataType.VARCHAR.toBytes(fam), PDataType.VARCHAR.toBytes(regionName));
            Put put = new Put(prefix);
            if (tracker.getGuidePosts(fam) != null) {
                put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES,
                        (tracker.getGuidePosts(fam)));
            }
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
                    PDataType.VARBINARY.toBytes(tracker.getMinKey(fam)));
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
                    PDataType.VARBINARY.toBytes(tracker.getMaxKey(fam)));
            puts.add(put);
            // serialize each of the metrics with the associated serializer
            statisticsTable.put(puts);
            statisticsTable.flushCommits();
        }
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