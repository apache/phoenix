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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

public class ScanBoundaryFunctionTest extends BaseConnectionlessQueryTest {

  @Test
  public void testScanStartKeyWithLiteral() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_START_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("startkey"));
    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);

    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("startkey"), scan.getStartRow());
  }

  @Test
  public void testScanEndKeyWithLiteral() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("endkey"));
    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);

    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStopRow());
    assertArrayEquals(Bytes.toBytes("endkey"), scan.getStopRow());
  }

  @Test
  public void testScanBothKeysWithLiterals() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("statz--__39gf04i583130~305i-4"));
    stmt.setBytes(2, Bytes.toBytes("##__39gf04i583130~305i-end-4"));
    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);

    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStartRow());
    assertNotNull(scan.getStopRow());
    assertArrayEquals(Bytes.toBytes("statz--__39gf04i583130~305i-4"), scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("##__39gf04i583130~305i-end-4"), scan.getStopRow());
  }

  @Test
  public void testScanStartKeyWithBindParameter() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_START_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("bindstart"));

    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("bindstart"), scan.getStartRow());
    assertArrayEquals(new byte[0], scan.getStopRow());
  }

  @Test
  public void testScanStartKeyWithBindParameter2() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("bindstart"));
    stmt.setBytes(2, null);

    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("bindstart"), scan.getStartRow());
    assertArrayEquals(new byte[0], scan.getStopRow());
  }

  @Test
  public void testScanEndKeyWithBindParameter() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("bindend"));

    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStopRow());
    assertArrayEquals(Bytes.toBytes("bindend"), scan.getStopRow());
    assertArrayEquals(new byte[0], scan.getStartRow());
  }

  @Test
  public void testScanEndKeyWithBindParameter2() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, new byte[0]);
    stmt.setBytes(2, Bytes.toBytes("bindend"));

    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStopRow());
    assertArrayEquals(Bytes.toBytes("bindend"), scan.getStopRow());
    assertArrayEquals(new byte[0], scan.getStartRow());
  }

  @Test
  public void testScanBothKeysWithBindParameters() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("bindstart"));
    stmt.setBytes(2, Bytes.toBytes("bindend"));

    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStartRow());
    assertNotNull(scan.getStopRow());
    assertArrayEquals(Bytes.toBytes("bindstart"), scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("bindend"), scan.getStopRow());
  }

  @Test
  public void testScanWithRegularWhereAndBoundary() throws Exception {
    Properties props = new Properties();
    PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);

    // This test verifies that scan boundary functions work alongside regular WHERE conditions
    String sql = "SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME = 'TEST' AND SCAN_END_KEY() = ?";
    PreparedStatement stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("boundary"));

    PhoenixPreparedStatement pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    QueryPlan plan = pstmt.optimizeQuery(sql);
    Scan scan = plan.getContext().getScan();

    assertNotNull(scan.getStopRow());
    assertNotNull(scan.getStartRow());
    assertArrayEquals(Bytes.toBytesBinary("\\x00\\x00TEST"), scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("boundary"), scan.getStopRow());

    sql =
      "SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME = 'TEST' AND SCAN_START_KEY() = ? AND SCAN_END_KEY() = ?";
    stmt = conn.prepareStatement(sql);
    stmt.setBytes(1, Bytes.toBytes("boundary"));
    stmt.setBytes(2, Bytes.toBytes("boundary"));

    pstmt = stmt.unwrap(PhoenixPreparedStatement.class);
    plan = pstmt.optimizeQuery(sql);
    scan = plan.getContext().getScan();

    assertNotNull(scan.getStopRow());
    assertNotNull(scan.getStartRow());
    assertArrayEquals(Bytes.toBytesBinary("\\x00\\x00TEST"), scan.getStartRow());
    assertArrayEquals(Bytes.toBytes("boundary"), scan.getStopRow());
  }
}
