/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelStatsDisabledTest.class)
public class PartialScannerResultsDisabledIT extends ParallelStatsDisabledIT {
    public static final String TEST_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS %s\n" + "(\n" + "    ORGANIZATION_ID CHAR(15) NOT NULL,\n"
                    + "    FEED_ELEMENT_ID CHAR(15) NOT NULL,\n"
                    + "    CONTAINER_ID CHAR(15) NOT NULL,\n"
                    + "    FEED_TYPE VARCHAR(1) NOT NULL, \n"
                    + "    NETWORK_ID CHAR(15) NOT NULL,\n" + "    USER_ID CHAR(15) NOT NULL,\n"
                    + "    CREATED_TIME TIMESTAMP,\n" + "    LAST_UPDATE TIMESTAMP,\n"
                    + "    RELEVANCE_SCORE DOUBLE,\n" + "    FEED_ITEM_TYPE VARCHAR(1),\n"
                    + "    FEED_ELEMENT_TYPE VARCHAR(1),\n"
                    + "    FEED_ELEMENT_IS_SYS_GEN BOOLEAN,\n"
                    + "    FEED_ELEMENT_STATUS VARCHAR(1),\n"
                    + "    FEED_ELEMENT_VISIBILITY VARCHAR(1),\n" + "    PARENT_ID CHAR(15),\n"
                    + "    CREATED_BY CHAR(15),\n" + "    BEST_COMMENT_ID CHAR(15),\n"
                    + "    COMMENT_COUNT INTEGER,\n" + "    CONSTRAINT PK PRIMARY KEY\n" + "    (\n"
                    + "        ORGANIZATION_ID,\n" + "        FEED_ELEMENT_ID,\n"
                    + "        CONTAINER_ID,\n" + "        FEED_TYPE,\n" + "        NETWORK_ID,\n"
                    + "        USER_ID\n" + "    )\n" + ") COLUMN_ENCODED_BYTES = 0";

    public static final String INDEX_1_DDL =
            "CREATE INDEX IF NOT EXISTS %s\n" + "ON %s (\n" + "    NETWORK_ID,\n"
                    + "    CONTAINER_ID,\n" + "    FEED_TYPE,\n" + "    USER_ID,\n"
                    + "    CREATED_TIME DESC,\n" + "    FEED_ELEMENT_ID DESC,\n"
                    + "    CREATED_BY\n" + ") "
                    + "    INCLUDE (\n" + "    FEED_ITEM_TYPE,\n"
                    + "    FEED_ELEMENT_TYPE,\n" + "    FEED_ELEMENT_IS_SYS_GEN,\n"
                    + "    FEED_ELEMENT_STATUS,\n" + "    FEED_ELEMENT_VISIBILITY,\n"
                    + "    PARENT_ID,\n" + "    BEST_COMMENT_ID,\n" + "    COMMENT_COUNT\n" + ")";

    private static final String UPSERT_INTO_DATA_TABLE =
            "UPSERT INTO %s\n" + "(\n" + "    ORGANIZATION_ID,\n" + "    FEED_ELEMENT_ID,\n"
                    + "    CONTAINER_ID,\n" + "    FEED_TYPE,\n" + "    NETWORK_ID,\n"
                    + "    USER_ID,\n" + "    CREATED_TIME,\n" + "    LAST_UPDATE,\n"
                    + "    FEED_ITEM_TYPE,\n" + "    FEED_ELEMENT_TYPE,\n"
                    + "    FEED_ELEMENT_IS_SYS_GEN,\n" + "    FEED_ELEMENT_STATUS,\n"
                    + "    FEED_ELEMENT_VISIBILITY,\n" + "    PARENT_ID,\n" + "    CREATED_BY,\n"
                    + "    BEST_COMMENT_ID,\n" + "    COMMENT_COUNT\n" + ")"
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private String dataTableName;
    private String indexTableName;
    private String schemaName;
    private String dataTableFullName;
    private static String indexTableFullName;
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PartialScannerResultsDisabledIT.class);
    private static Random random = new Random(1);
    // background writer threads
    private static Random sourceOfRandomness = new Random(0);
    private static AtomicInteger upsertIdCounter = new AtomicInteger(1);

    @Before
    public void setup() throws Exception {
        // create the tables
        generateUniqueTableNames();
        createTestTable(getUrl(), String.format(TEST_TABLE_DDL, dataTableFullName));
        createTestTable(getUrl(), String.format(INDEX_1_DDL, indexTableName, dataTableFullName));
    }
    
    @Test
    public void testWithEnoughData() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Write enough data to trigger partial scanner results
            // TODO: it's likely that less data could be written if whatever
            // config parameters decide this are lowered.
            writeSingleBatch(conn, 100, 20, dataTableFullName);
            LOGGER.info("Running scrutiny");
            // Scutunize index to see if partial results are silently returned
            // In that case we'll get a false positive on the scrutiny run.
            long rowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(2000,rowCount);
        }
    }

    /**
     * Simple select query with fetch size that exceed the result size. In that case scan would start to produce
     * partial result sets that from Phoenix perspective are the rows with NULL values.
     * @throws SQLException
     */
    @Test
    public void partialResultDuringSelect () throws SQLException {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, "5");
        int numRecords = 10;
        try (Connection conn = DriverManager.getConnection(url, props)) {
            conn.createStatement().execute(
                    "CREATE TABLE " + tableName + " (PK1 INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR)");
            int i = 0;
            String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            while (i < numRecords) {
                stmt.setInt(1, i);
                stmt.setString(2, UUID.randomUUID().toString());
                stmt.executeUpdate();
                i++;
            }
            conn.commit();

            String sql = "SELECT * FROM " + tableName;
            // at every next call wait for this period. This will cause lease to expire.
            Statement s = conn.createStatement();
            s.setFetchSize(100);
            ResultSet rs = s.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                if (rs.getString(2) == null)
                    fail("Null value because of partial row scan");
            }
            count++;
        }

    }

    private static String randString(int length, Random random) {
        return RandomStringUtils.randomAlphabetic(length);
    }
    
    public static void writeSingleBatch(Connection connection, int batchSize, int numBatches, String tableName) throws Exception {
        for (int j = 0; j < numBatches; j++) {
            try (PreparedStatement statement =
                    connection.prepareStatement(String.format(UPSERT_INTO_DATA_TABLE, tableName))) {
                for (int i = 0; i < batchSize; i++) {
                    int index = 0;
                    String id = "" + upsertIdCounter.getAndIncrement();
                    statement.setString(++index, id); // ORGANIZATION_ID
                    statement.setString(++index, id); // FEED_ELEMENT_ID,\n"
                    statement.setString(++index, id); // CONTAINER_ID,\n"
                    statement.setString(++index, randString(1, sourceOfRandomness)); // FEED_TYPE,\n"
                    statement.setString(++index, randString(15, sourceOfRandomness)); // NETWORK_ID,\n"
                    statement.setString(++index, randString(15, sourceOfRandomness)); // USER_ID,\n"
                    statement.setTimestamp(++index, new Timestamp(System.currentTimeMillis())); // CREATED_TIME,\n"
                    statement.setTimestamp(++index, new Timestamp(System.currentTimeMillis())); // LAST_UPDATE\n"
                    statement.setString(++index, randString(1, sourceOfRandomness)); // FEED_ITEM_TYPE\n"
                    statement.setString(++index, randString(1, sourceOfRandomness)); // FEED_ELEMENT_TYPE\n"
                    statement.setBoolean(++index, false); // FEED_ELEMENT_IS_SYS_GEN\n"
                    statement.setString(++index, randString(1, sourceOfRandomness)); // FEED_ELEMENT_STATUS\n"
                    statement.setString(++index, randString(1, sourceOfRandomness)); // FEED_ELEMENT_VISIBILITY\n"
                    statement.setString(++index, randString(15, sourceOfRandomness)); // PARENT_ID\n"
                    statement.setString(++index, randString(15, sourceOfRandomness)); // CREATED_BY\n"
                    statement.setString(++index, randString(15, sourceOfRandomness)); // BEST_COMMENT_ID\n"
                    statement.setInt(++index, random.nextInt()); // COMMENT_COUNT\n" + ")"
                    statement.execute();
                }
                connection.commit();
            }
        }
    }
    
    private void generateUniqueTableNames() {
        schemaName = generateUniqueName();
        dataTableName = generateUniqueName() + "_DATA";
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        indexTableName = generateUniqueName() + "_IDX";
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
    }

}
