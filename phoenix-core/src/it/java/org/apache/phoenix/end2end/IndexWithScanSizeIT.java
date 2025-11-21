/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexWithScanSizeIT {

  private static final String BASE_TABLE_NAME = "Table012";
  private static final String INDEX_NAME = "Index012";
  private static final int NUM_RECORDS = 18000;

  private static String url;
  private static HBaseTestingUtility utility = null;
  private static String tmpDir;
  private static Connection conn;

  @Rule
  public final TestName testName = new TestName();

  private final boolean isCoveredIndex;
  private String tableName;
  private static final Properties props = new Properties();

  @Parameterized.Parameters(name = "isCoveredIndex={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  public IndexWithScanSizeIT(boolean isCoveredIndex) {
    this.isCoveredIndex = isCoveredIndex;
    this.tableName = BASE_TABLE_NAME + "_" + (isCoveredIndex ? "covered" : "uncovered");
  }

  @BeforeClass
  public static void initialize() throws Exception {
    tmpDir = System.getProperty("java.io.tmpdir");
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.wal.provider", "filesystem");
    utility = new HBaseTestingUtility(conf);
    setUpConfigForMiniCluster(conf);

    utility.startMiniCluster();
    String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
    url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

    props.put("hbase.client.scanner.max.result.size", "1048");
    conn = DriverManager.getConnection(url, props);
  }

  @Before
  public void setup() throws Exception {
    createTableAndInsertData();
  }

  @After
  public void tearDown() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS XYZ.\"" + tableName + "\"");
      conn.commit();
    }
  }

  private void createTableAndInsertData() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      String createTableSQL = "CREATE TABLE IF NOT EXISTS XYZ.\"" + tableName + "\" "
        + "(\"pk\" VARCHAR NOT NULL, \"sk\" DOUBLE NOT NULL, COL BSON "
        + "CONSTRAINT pk PRIMARY KEY (\"pk\",\"sk\"))";
      stmt.execute(createTableSQL);
      conn.commit();

      String createIndexSQL;
      if (isCoveredIndex) {
        createIndexSQL = "CREATE INDEX IF NOT EXISTS \"" + tableName + "_" + INDEX_NAME + "\" "
          + "ON XYZ.\"" + tableName + "\" "
          + "(BSON_VALUE(COL,'status','VARCHAR'),BSON_VALUE(COL,'timestamp','DOUBLE')) "
          + "INCLUDE (COL) " + "WHERE BSON_VALUE(COL,'status','VARCHAR') IS NOT NULL "
          + "AND BSON_VALUE(COL,'timestamp','DOUBLE') IS NOT NULL ";
      } else {
        createIndexSQL = "CREATE UNCOVERED INDEX IF NOT EXISTS \"" + tableName + "_" + INDEX_NAME
          + "\" " + "ON XYZ.\"" + tableName + "\" "
          + "(BSON_VALUE(COL,'status','VARCHAR'),BSON_VALUE(COL,'timestamp','DOUBLE')) "
          + "WHERE BSON_VALUE(COL,'status','VARCHAR') IS NOT NULL "
          + "AND BSON_VALUE(COL,'timestamp','DOUBLE') IS NOT NULL ";
      }
      stmt.execute(createIndexSQL);
      conn.commit();
    }

    String upsertSQL = "UPSERT INTO XYZ.\"" + tableName + "\" VALUES (?,?,?)";

    try (PreparedStatement pstmt = conn.prepareStatement(upsertSQL)) {
      for (int i = 0; i < NUM_RECORDS; i++) {
        BsonDocument doc = generateBsonDocument(i);

        pstmt.setString(1, "pk_" + (i % 100));
        pstmt.setDouble(2, i);
        pstmt.setObject(3, doc);
        pstmt.executeUpdate();

        if (i % 1000 == 0) {
          conn.commit();
        }
      }
      conn.commit();
    }

    String upsertUpdateSQL = "UPSERT INTO XYZ.\"" + tableName + "\" VALUES (?,?,?) "
      + "ON DUPLICATE KEY UPDATE COL = BSON_UPDATE_EXPRESSION(COL,?)";

    try (PreparedStatement pstmt = conn.prepareStatement(upsertUpdateSQL)) {
      for (int i = 0; i < NUM_RECORDS / 20; i++) {
        BsonDocument doc = generateBsonDocument(i);

        BsonDocument updateExp = new BsonDocument().append("$SET",
          new BsonDocument().append("status", new BsonString("updated_status"))
            .append("timestamp", new BsonDouble(i + 80000))
            .append("amount", new BsonDouble(i + 3000)));

        pstmt.setString(1, "pk_" + (i % 100));
        pstmt.setDouble(2, i);
        pstmt.setObject(3, doc);
        pstmt.setObject(4, updateExp);
        pstmt.executeUpdate();

        if (i % 500 == 0) {
          conn.commit();
        }
      }
      conn.commit();
    }

  }

  private static BsonDocument generateBsonDocument(int i) {
    BsonDocument doc = new BsonDocument();

    doc.put("status", new BsonString("status_" + (i % 10)));
    doc.put("timestamp", new BsonDouble(i));
    doc.put("amount", new BsonDouble((i % 1000) + 1));
    doc.put("priority", new BsonDouble(i % 10));
    doc.put("active", new BsonBoolean(i % 2 == 0));
    doc.put("description", new BsonString("description_" + i));

    BsonArray labelsArray = new BsonArray();
    labelsArray.add(new BsonString("label_" + (i % 6)));
    labelsArray.add(new BsonString("shared_label"));
    doc.put("labels", labelsArray);

    BsonArray scoresArray = new BsonArray();
    scoresArray.add(new BsonDouble(i % 15));
    scoresArray.add(new BsonDouble((i % 25) + 100));
    doc.put("scores", scoresArray);

    BsonArray dataArray = new BsonArray();
    dataArray.add(new BsonBinary(new byte[] { (byte) (i % 64), 0x01 }));
    dataArray.add(new BsonBinary(new byte[] { 0x0A, 0x0B }));
    doc.put("data", dataArray);

    if (i % 5 == 0) {
      doc.put("optional_field", new BsonString("optional_" + i));
    }

    BsonDocument nestedMap = new BsonDocument();
    nestedMap.put("key1", new BsonString("value_" + (i % 4)));
    nestedMap.put("key2", new BsonDouble(i % 80));
    doc.put("config", nestedMap);

    BsonArray nestedList = new BsonArray();
    nestedList.add(new BsonString("list_item_" + (i % 8)));
    nestedList.add(new BsonDouble(i % 40));
    BsonArray setInList = new BsonArray();
    setInList.add(new BsonString("set_in_list_" + (i % 3)));
    setInList.add(new BsonString("list_common"));
    nestedList.add(setInList);
    doc.put("itms", nestedList);

    return doc;
  }

  @AfterClass
  public static void stop() throws Exception {
    if (conn != null) {
      conn.close();
    }
    ServerUtil.ConnectionFactory.shutdown();
    try {
      DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    } finally {
      if (utility != null) {
        utility.shutdownMiniCluster();
      }
      ServerMetadataCacheTestImpl.resetCache();
    }
    System.setProperty("java.io.tmpdir", tmpDir);
  }

  @Test(timeout = 300000)
  public void testScanWithBetweenCondition() throws Exception {
    String query = "SELECT /*+ INDEX(\"XYZ." + tableName + "\" \"" + tableName + "_" + INDEX_NAME
      + "\") */ " + "COL FROM XYZ.\"" + tableName + "\" " + "WHERE BSON_CONDITION_EXPRESSION(COL, "
      + "'{\"$EXPR\": \"amount BETWEEN :low AND :high\", "
      + "\"$VAL\": {\":low\": 3000, \":high\": 4000}}') "
      + "ORDER BY BSON_VALUE(COL,'status','VARCHAR'),"
      + " BSON_VALUE(COL,'timestamp','DOUBLE') LIMIT 1000";

    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
      int count = 0;

      while (rs.next()) {
        BsonDocument doc = (RawBsonDocument) rs.getObject(1);

        double amount = doc.getDouble("amount").getValue();

        Assert.assertTrue("Amount should be >= 3000, got " + amount, amount >= 3000);
        Assert.assertTrue("Amount should be <= 4000, got " + amount, amount <= 4000);
        count++;
      }
      Assert.assertEquals("Should find exactly 900 records with amount in range", 900, count);
    }
  }

}
