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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for TOTAL_SEGMENTS()
 */
public class TotalSegmentsFunctionIT extends ParallelStatsDisabledIT {

  private String fullTableName;

  @Before
  public void setUp() throws Exception {
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    fullTableName = schemaName + "." + tableName;
  }

  @Test
  public void testTotalSegmentsWithSimpleTable() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with multiple splits
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON ('B', 'D', 'F')";

      conn.createStatement().execute(createSql);

      // Get actual regions from ConnectionQueryServices
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      // Execute TOTAL_SEGMENTS query
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 95";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> queryRegions = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          queryRegions.add(new RegionInfo(startKey, endKey));
        }

        // Verify we get the same number of regions
        assertEquals("Number of regions should match", actualRegions.size(), queryRegions.size());

        // Verify each region's start and end keys match
        for (int i = 0; i < actualRegions.size(); i++) {
          HRegionLocation actualRegion = actualRegions.get(i);
          RegionInfo queryRegion = queryRegions.get(i);

          byte[] expectedStart = actualRegion.getRegion().getStartKey();
          byte[] expectedEnd = actualRegion.getRegion().getEndKey();

          assertArrayEquals("Start key should match for region " + i, expectedStart,
            queryRegion.startKey);
          assertArrayEquals("End key should match for region " + i, expectedEnd,
            queryRegion.endKey);
        }
      }
    }
  }

  @Test
  public void testTotalSegmentsWithCompositeKey() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with composite primary key and splits
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK1 INTEGER NOT NULL, "
        + "PK2 VARCHAR NOT NULL, " + "V1 VARCHAR, " + "CONSTRAINT PK PRIMARY KEY (PK1, PK2)"
        + ") SPLIT ON ((1,'B'), (2,'A'), (3,'C'))";

      conn.createStatement().execute(createSql);

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      // Execute TOTAL_SEGMENTS query
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 50";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> queryRegions = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          queryRegions.add(new RegionInfo(startKey, endKey));
        }

        // Verify region count and boundaries
        assertEquals("Number of regions should match", actualRegions.size(), queryRegions.size());

        for (int i = 0; i < actualRegions.size(); i++) {
          HRegionLocation actualRegion = actualRegions.get(i);
          RegionInfo queryRegion = queryRegions.get(i);

          assertArrayEquals("Start key should match for composite key region " + i,
            actualRegion.getRegion().getStartKey(), queryRegion.startKey);
          assertArrayEquals("End key should match for composite key region " + i,
            actualRegion.getRegion().getEndKey(), queryRegion.endKey);
        }
      }
    }
  }

  @Test
  public void testTotalSegmentsWithSingleRegion() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table without splits (single region)
      String createSql =
        "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, " + "V1 VARCHAR" + ")";

      conn.createStatement().execute(createSql);

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      // Should have exactly one region
      assertEquals("Single region table should have one region", 1, actualRegions.size());

      // Execute TOTAL_SEGMENTS query
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 1";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        assertTrue("Should have exactly one result row", rs.next());

        byte[] queryStartKey = rs.getBytes(1);
        byte[] queryEndKey = rs.getBytes(2);
        queryStartKey = queryStartKey == null ? new byte[0] : queryStartKey;
        queryEndKey = queryEndKey == null ? new byte[0] : queryEndKey;

        HRegionLocation singleRegion = actualRegions.get(0);
        byte[] expectedStartKey = singleRegion.getRegion().getStartKey();
        byte[] expectedEndKey = singleRegion.getRegion().getEndKey();

        assertArrayEquals("Start key should match for single region", expectedStartKey,
          queryStartKey);
        assertArrayEquals("End key should match for single region", expectedEndKey, queryEndKey);

        // Verify no more rows
        assertTrue("Should have only one result row", !rs.next());
      }
    }
  }

  @Test
  public void testTotalSegmentsWithManyRegions() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with many splits
      String[] splits = { "'10'", "'20'", "'30'", "'40'", "'50'", "'60'", "'70'", "'80'", "'90'" };
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON (" + String.join(", ", splits) + ")";

      conn.createStatement().execute(createSql);

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      // Should have 10 regions (9 splits + 1)
      assertEquals("Should have 10 regions with 9 splits", 10, actualRegions.size());

      // Execute TOTAL_SEGMENTS query
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 100";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> queryRegions = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          queryRegions.add(new RegionInfo(startKey, endKey));
        }

        assertEquals("Should return 10 regions", 10, queryRegions.size());

        // Verify all regions match
        for (int i = 0; i < actualRegions.size(); i++) {
          HRegionLocation actualRegion = actualRegions.get(i);
          RegionInfo queryRegion = queryRegions.get(i);

          assertArrayEquals("Start key should match for region " + i + " in many-region table",
            actualRegion.getRegion().getStartKey(), queryRegion.startKey);
          assertArrayEquals("End key should match for region " + i + " in many-region table",
            actualRegion.getRegion().getEndKey(), queryRegion.endKey);
        }
      }
    }
  }

  @Test
  public void testTotalSegmentsDoesNotGoToServer() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON ('B', 'D')";

      conn.createStatement().execute(createSql);

      // Insert some data so we can verify the query doesn't scan it
      try (PreparedStatement insert =
        conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES (?, ?)")) {
        insert.setString(1, "A");
        insert.setString(2, "ValueA");
        insert.executeUpdate();
        insert.setString(1, "C");
        insert.setString(2, "ValueC");
        insert.executeUpdate();
        insert.setString(1, "E");
        insert.setString(2, "ValueE");
        insert.executeUpdate();
        conn.commit();
      }

      // Execute TOTAL_SEGMENTS query - this should NOT scan the inserted data
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 42";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        int regionCount = 0;
        while (rs.next()) {
          regionCount++;
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;

          // Verify that we get region boundary data, not row data
          assertNotNull("Start key should not be null", startKey);
          assertNotNull("End key should not be null", endKey);
        }

        // Should get 3 regions (2 splits + 1)
        assertEquals("Should return 3 regions from client-side region scan", 3, regionCount);
      }
    }
  }

  @Test
  public void testTotalSegmentsWithRegionBucketing() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with 12 regions (11 splits)
      String[] splits =
        { "'10'", "'20'", "'30'", "'40'", "'50'", "'60'", "'70'", "'80'", "'90'", "'A0'", "'B0'" };
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON (" + String.join(", ", splits) + ")";

      conn.createStatement().execute(createSql);

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      // Should have 12 regions (11 splits + 1)
      assertEquals("Should have 12 regions with 11 splits", 12, actualRegions.size());

      // Test with TOTAL_SEGMENTS() = 4 (less than actual regions)
      // This should bucket 12 regions into 4 segments
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 4";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> segments = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          segments.add(new RegionInfo(startKey, endKey));
        }

        // Should get exactly 4 segments
        assertEquals("Should return 4 segments when bucketing 12 regions", 4, segments.size());

        // Verify bucketing logic: 12 regions / 4 buckets = 3 regions per bucket
        // Bucket 0: regions 0,1,2 -> start=region[0].start, end=region[2].end
        assertArrayEquals("First segment should start with first region's start key",
          actualRegions.get(0).getRegion().getStartKey(), segments.get(0).startKey);
        assertArrayEquals("First segment should end with third region's end key",
          actualRegions.get(2).getRegion().getEndKey(), segments.get(0).endKey);

        // Bucket 1: regions 3,4,5 -> start=region[3].start, end=region[5].end
        assertArrayEquals("Second segment should start with fourth region's start key",
          actualRegions.get(3).getRegion().getStartKey(), segments.get(1).startKey);
        assertArrayEquals("Second segment should end with sixth region's end key",
          actualRegions.get(5).getRegion().getEndKey(), segments.get(1).endKey);

        // Bucket 2: regions 6,7,8 -> start=region[6].start, end=region[8].end
        assertArrayEquals("Third segment should start with seventh region's start key",
          actualRegions.get(6).getRegion().getStartKey(), segments.get(2).startKey);
        assertArrayEquals("Third segment should end with ninth region's end key",
          actualRegions.get(8).getRegion().getEndKey(), segments.get(2).endKey);

        // Bucket 3: regions 9,10,11 -> start=region[9].start, end=region[11].end
        assertArrayEquals("Fourth segment should start with tenth region's start key",
          actualRegions.get(9).getRegion().getStartKey(), segments.get(3).startKey);
        assertArrayEquals("Fourth segment should end with twelfth region's end key",
          actualRegions.get(11).getRegion().getEndKey(), segments.get(3).endKey);
      }
    }
  }

  @Test
  public void testTotalSegmentsWithUnevenBucketing() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with 10 regions (9 splits)
      String[] splits = { "'10'", "'20'", "'30'", "'40'", "'50'", "'60'", "'70'", "'80'", "'90'" };
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON (" + String.join(", ", splits) + ")";

      conn.createStatement().execute(createSql);

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      assertEquals("Should have 10 regions", 10, actualRegions.size());

      // Test with TOTAL_SEGMENTS() = 3 (uneven division: 10/3 = 3 remainder 1)
      // This should create: bucket1=4 regions, bucket2=3 regions, bucket3=3 regions
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 3";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> segments = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          segments.add(new RegionInfo(startKey, endKey));
        }

        assertEquals("Should return 3 segments", 3, segments.size());

        // Verify uneven bucketing: q=3, r=1
        // Bucket 0 (size=4): regions 0,1,2,3
        assertArrayEquals("First segment should start with first region's start key",
          actualRegions.get(0).getRegion().getStartKey(), segments.get(0).startKey);
        assertArrayEquals("First segment should end with fourth region's end key",
          actualRegions.get(3).getRegion().getEndKey(), segments.get(0).endKey);

        // Bucket 1 (size=3): regions 4,5,6
        assertArrayEquals("Second segment should start with fifth region's start key",
          actualRegions.get(4).getRegion().getStartKey(), segments.get(1).startKey);
        assertArrayEquals("Second segment should end with seventh region's end key",
          actualRegions.get(6).getRegion().getEndKey(), segments.get(1).endKey);

        // Bucket 2 (size=3): regions 7,8,9
        assertArrayEquals("Third segment should start with eighth region's start key",
          actualRegions.get(7).getRegion().getStartKey(), segments.get(2).startKey);
        assertArrayEquals("Third segment should end with tenth region's end key",
          actualRegions.get(9).getRegion().getEndKey(), segments.get(2).endKey);
      }
    }
  }

  @Test
  public void testTotalSegmentsWithLargeTableSmallSegments() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with 20 regions (19 splits)
      List<String> splitList = new ArrayList<>();
      for (int i = 1; i < 20; i++) {
        splitList.add(String.format("'%02d'", i * 5));
      }
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON (" + String.join(", ", splitList) + ")";

      conn.createStatement().execute(createSql);

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      assertEquals("Should have 20 regions", 20, actualRegions.size());

      // Test with TOTAL_SEGMENTS() = 6 (20 regions -> 6 segments)
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 6";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> segments = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          segments.add(new RegionInfo(startKey, endKey));
        }

        assertEquals("Should return 6 segments when bucketing 20 regions", 6, segments.size());

        // Verify detailed bucketing logic: 20 regions / 6 segments
        // q = 20/6 = 3, r = 20%6 = 2
        // First 2 segments get 4 regions each, remaining 4 segments get 3 regions each

        // Segment 0: regions 0,1,2,3 (4 regions)
        assertArrayEquals("Segment 0 should start with region 0's start key",
          actualRegions.get(0).getRegion().getStartKey(), segments.get(0).startKey);
        assertArrayEquals("Segment 0 should end with region 3's end key",
          actualRegions.get(3).getRegion().getEndKey(), segments.get(0).endKey);

        // Segment 1: regions 4,5,6,7 (4 regions)
        assertArrayEquals("Segment 1 should start with region 4's start key",
          actualRegions.get(4).getRegion().getStartKey(), segments.get(1).startKey);
        assertArrayEquals("Segment 1 should end with region 7's end key",
          actualRegions.get(7).getRegion().getEndKey(), segments.get(1).endKey);

        // Segment 2: regions 8,9,10 (3 regions)
        assertArrayEquals("Segment 2 should start with region 8's start key",
          actualRegions.get(8).getRegion().getStartKey(), segments.get(2).startKey);
        assertArrayEquals("Segment 2 should end with region 10's end key",
          actualRegions.get(10).getRegion().getEndKey(), segments.get(2).endKey);

        // Segment 3: regions 11,12,13 (3 regions)
        assertArrayEquals("Segment 3 should start with region 11's start key",
          actualRegions.get(11).getRegion().getStartKey(), segments.get(3).startKey);
        assertArrayEquals("Segment 3 should end with region 13's end key",
          actualRegions.get(13).getRegion().getEndKey(), segments.get(3).endKey);

        // Segment 4: regions 14,15,16 (3 regions)
        assertArrayEquals("Segment 4 should start with region 14's start key",
          actualRegions.get(14).getRegion().getStartKey(), segments.get(4).startKey);
        assertArrayEquals("Segment 4 should end with region 16's end key",
          actualRegions.get(16).getRegion().getEndKey(), segments.get(4).endKey);

        // Segment 5: regions 17,18,19 (3 regions)
        assertArrayEquals("Segment 5 should start with region 17's start key",
          actualRegions.get(17).getRegion().getStartKey(), segments.get(5).startKey);
        assertArrayEquals("Segment 5 should end with region 19's end key",
          actualRegions.get(19).getRegion().getEndKey(), segments.get(5).endKey);

        // Verify that segments don't overlap and cover all regions
        for (int i = 0; i < segments.size() - 1; i++) {
          // End key of segment[i] should be >= start key of segment[i+1]
          // (they might be equal for adjacent segments)
          assertTrue("Segments should be ordered correctly",
            Bytes.compareTo(segments.get(i).endKey, segments.get(i + 1).startKey) <= 0);
        }
      }
    }
  }

  @Test
  public void testTotalSegmentsEdgeCase() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with 5 regions
      String[] splits = { "'20'", "'40'", "'60'", "'80'" };
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR" + ") SPLIT ON (" + String.join(", ", splits) + ")";

      conn.createStatement().execute(createSql);

      // Test edge case: TOTAL_SEGMENTS() = 1 (all regions combined into one segment)
      String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 1";

      try (PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {

        assertTrue("Should have exactly one result", rs.next());

        byte[] startKey = rs.getBytes(1);
        byte[] endKey = rs.getBytes(2);
        startKey = startKey == null ? new byte[0] : startKey;
        endKey = endKey == null ? new byte[0] : endKey;

        // Get actual regions for verification
        PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
        ConnectionQueryServices services = phoenixConn.getQueryServices();
        byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
        List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

        // Single segment should span from first region's start to last region's end
        assertArrayEquals("Single segment should start with first region's start key",
          actualRegions.get(0).getRegion().getStartKey(), startKey);
        assertArrayEquals("Single segment should end with last region's end key",
          actualRegions.get(actualRegions.size() - 1).getRegion().getEndKey(), endKey);

        // Should have no more results
        assertTrue("Should have only one result", !rs.next());
      }
    }
  }

  @Test
  public void testTotalSegmentsWithDataDistribution() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with 15 regions (14 splits) for good bucketing examples
      String[] splits = { "'10'", "'20'", "'30'", "'40'", "'50'", "'60'", "'70'", "'80'", "'90'",
        "'A0'", "'B0'", "'C0'", "'D0'", "'E0'" };
      String createSql = "CREATE TABLE " + fullTableName + " (" + "PK VARCHAR PRIMARY KEY, "
        + "V1 VARCHAR, V2 INTEGER" + ") SPLIT ON (" + String.join(", ", splits) + ")";

      conn.createStatement().execute(createSql);

      // Insert data across different regions
      try (PreparedStatement insert =
        conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES (?, ?, ?)")) {
        String[] testKeys =
          { "05", "15", "25", "35", "45", "55", "65", "75", "85", "95", "A5", "B5", "C5", "D5",
            "E5", "F5", "2X2903hg", "5Ywoe", "EeEe45", "20", "50", "500", "90", "10" };
        for (int i = 0; i < testKeys.length; i++) {
          insert.setString(1, testKeys[i]);
          insert.setString(2, "Value" + i);
          insert.setInt(3, i * 10);
          insert.executeUpdate();
        }
        conn.commit();
      }

      // Get actual regions
      PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
      ConnectionQueryServices services = phoenixConn.getQueryServices();
      byte[] physicalTableName = phoenixConn.getTable(fullTableName).getPhysicalName().getBytes();
      List<HRegionLocation> actualRegions = services.getAllTableRegions(physicalTableName, 30000);

      assertEquals("Should have 15 regions", 15, actualRegions.size());

      // Test case 1: Bucket 15 regions into 5 segments (3 regions per segment)
      String sql1 = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 5";

      try (PreparedStatement stmt = conn.prepareStatement(sql1);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> segments = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          segments.add(new RegionInfo(startKey, endKey));
        }

        assertEquals("Should return 5 segments", 5, segments.size());

        // Verify 15/5 = 3 regions per segment
        // Segment 0: regions 0,1,2
        assertArrayEquals("First segment should start with first region",
          actualRegions.get(0).getRegion().getStartKey(), segments.get(0).startKey);
        assertArrayEquals("First segment should end with third region",
          actualRegions.get(2).getRegion().getEndKey(), segments.get(0).endKey);

        // Segment 4: regions 12,13,14
        assertArrayEquals("Last segment should start with thirteenth region",
          actualRegions.get(12).getRegion().getStartKey(), segments.get(4).startKey);
        assertArrayEquals("Last segment should end with fifteenth region",
          actualRegions.get(14).getRegion().getEndKey(), segments.get(4).endKey);
      }

      // Test case 2: Bucket 15 regions into 7 segments (uneven distribution)
      // 15/7 = 2 remainder 1, so first 1 bucket gets 3 regions, rest get 2
      String sql2 = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
        + " WHERE TOTAL_SEGMENTS() = 7";

      try (PreparedStatement stmt = conn.prepareStatement(sql2);
        ResultSet rs = stmt.executeQuery()) {

        List<RegionInfo> segments = new ArrayList<>();
        while (rs.next()) {
          byte[] startKey = rs.getBytes(1);
          byte[] endKey = rs.getBytes(2);
          startKey = startKey == null ? new byte[0] : startKey;
          endKey = endKey == null ? new byte[0] : endKey;
          segments.add(new RegionInfo(startKey, endKey));
        }

        assertEquals("Should return 7 segments for uneven bucketing", 7, segments.size());

        assertArrayEquals("First segment should span 3 regions",
          actualRegions.get(2).getRegion().getEndKey(), segments.get(0).endKey);

        assertArrayEquals("Second segment should start with fourth region",
          actualRegions.get(3).getRegion().getStartKey(), segments.get(1).startKey);
        assertArrayEquals("Second segment should span 2 regions",
          actualRegions.get(4).getRegion().getEndKey(), segments.get(1).endKey);
      }

      // Full table scan
      String dataQuery = "SELECT PK, V1, V2 FROM " + fullTableName;
      int totalRowsFound = 0;
      List<String> totalData = new ArrayList<>();
      try (PreparedStatement dataStmt = conn.prepareStatement(dataQuery)) {
        try (ResultSet dataRs = dataStmt.executeQuery()) {
          while (dataRs.next()) {
            String pk = dataRs.getString(1);
            String v1 = dataRs.getString(2);
            int v2 = dataRs.getInt(3);
            totalRowsFound++;
            totalData.add(String.format("PK=%s, V1=%s, V2=%d", pk, v1, v2));
          }
        }
      }
      assertEquals("Total 24 rows", 24, totalRowsFound);

      for (int segment = 1; segment < 20; segment++) {
        String sql = "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM " + fullTableName
          + " WHERE TOTAL_SEGMENTS() = " + segment;
        List<String> segmentData = new ArrayList<>();
        assertEquals("Total 24 rows", 24, getTotalRowsFound(conn, sql, segmentData));
        assertEquals("All rows should be matching with full table scan", totalData, segmentData);
      }
    }
  }

  private int getTotalRowsFound(Connection conn, String sql3, List<String> segmentData1)
    throws SQLException {
    int totalRowsFound = 0;
    try (PreparedStatement stmt = conn.prepareStatement(sql3); ResultSet rs = stmt.executeQuery()) {

      while (rs.next()) {
        byte[] segmentStart = rs.getBytes(1);
        byte[] segmentEnd = rs.getBytes(2);

        // Use the segment boundaries to query data with scan boundary functions
        String dataQuery = "SELECT PK, V1, V2 FROM " + fullTableName
          + " WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";

        try (PreparedStatement dataStmt = conn.prepareStatement(dataQuery)) {
          dataStmt.setBytes(1, segmentStart);
          dataStmt.setBytes(2, segmentEnd);
          try (ResultSet dataRs = dataStmt.executeQuery()) {
            while (dataRs.next()) {
              String pk = dataRs.getString(1);
              String v1 = dataRs.getString(2);
              int v2 = dataRs.getInt(3);
              totalRowsFound++;
              segmentData1.add(String.format("PK=%s, V1=%s, V2=%d", pk, v1, v2));
            }
          }
        }
      }
    }
    return totalRowsFound;
  }

  /**
   * Helper class to store region information for comparison
   */
  private static class RegionInfo {
    final byte[] startKey;
    final byte[] endKey;

    RegionInfo(byte[] startKey, byte[] endKey) {
      this.startKey = startKey;
      this.endKey = endKey;
    }

    @Override
    public String toString() {
      return "RegionInfo{startKey=" + Arrays.toString(startKey) + ", endKey="
        + Arrays.toString(endKey) + "}";
    }
  }
}
