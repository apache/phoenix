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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.GUIDE_POSTS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MAX_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.MIN_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.REGION_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

import java.io.IOException;
import java.sql.Date;
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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
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
            TABLE_SCHEM+ ","+
            TABLE_NAME + "," +
            COLUMN_FAMILY + "," +
            REGION_NAME + "," +
            LAST_STATS_UPDATE_TIME + "," +
            MIN_KEY + "," +
            MAX_KEY + "," +
            GUIDE_POSTS  +
            ") VALUES (?, ?, ?, ?, ?, ?, ? , ?)";
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
     * @param tablekey - The table name
     * @param schemaName - the schema name associated with the table          
     * @param region name -  the region of the table for which the stats are collected
     * @param tracker - the statistics tracker
     * @param fam -  the family for which the stats is getting collected.
     * @param url - the connection url - not null if coming from ANALYZE <tablename> query, else null
     * @param r 
     * @param l 
     * @throws IOException
     *             if we fail to do any of the puts. Any single failure will prevent any future attempts for the remaining list of stats to
     *             update
     */
    public void updateStats(String tableName, String regionName, StatisticsTracker tracker, String fam,
            String url, boolean split) throws IOException {
        if (tracker == null) { return; }

        long currentTime = timeKeeper.getCurrentTime();
        List<Mutation> mutations = new ArrayList<Mutation>();
        // Add the timestamp header
        byte[] prefix = StatisticsUtils.getRowKeyForTSUpdate(PDataType.VARCHAR.toBytes(tableName));
        Put put = new Put(prefix);
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.LAST_STATS_UPDATE_TIME_BYTES,
                PDataType.DATE.toBytes(new Date(currentTime)));
        statisticsTable.put(put);
        statisticsTable.flushCommits();

        prefix = StatisticsUtils.getRowKey(PDataType.VARCHAR.toBytes(tableName), PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        // Better to add them in in one batch using mutateRow
        if (!split) {
            Delete d = new Delete(prefix);
            d.deleteFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, currentTime - 1);
            mutations.add(d);
            //statisticsTable.delete(d);
        }
        put = new Put(prefix, currentTime);
        if (tracker.getGuidePosts(fam) != null) {
            put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.GUIDE_POSTS_BYTES,
                    currentTime, (tracker.getGuidePosts(fam)));
        }
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MIN_KEY_BYTES,
                currentTime, PDataType.VARBINARY.toBytes(tracker.getMinKey(fam)));
        put.add(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.MAX_KEY_BYTES,
                currentTime, PDataType.VARBINARY.toBytes(tracker.getMaxKey(fam)));
        mutations.add(put);
//        statisticsTable.put(puts);
  //      mutations.add(put);
        Object[] res = new Object[mutations.size()];
        try {
            statisticsTable.batch(mutations, res);
        } catch (InterruptedException e) {
            throw new IOException("Exception while adding deletes and puts");
        }
        //statisticsTable.mutateRow(mutations);
        // serialize each of the metrics with the associated serializer
        //statisticsTable.put(puts);
        statisticsTable.flushCommits();
    }
    
    public void deleteStats(String tableName, String regionName, StatisticsTracker tracker, String fam)
            throws IOException {
        byte[] prefix = StatisticsUtils.getRowKey(PDataType.VARCHAR.toBytes(tableName), PDataType.VARCHAR.toBytes(fam),
                PDataType.VARCHAR.toBytes(regionName));
        RowMutations mutations = new RowMutations(prefix);
        Delete d = new Delete(prefix);
        d.deleteFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, TimeKeeper.SYSTEM.getCurrentTime() - 1);
        mutations.add(d);
        statisticsTable.delete(d);
        //statisticsTable.mutateRow(mutations);
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