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
package org.apache.phoenix.schema;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class MappingTableDataTypeTest extends BaseTest{

  private static final Log LOG = LogFactory.getLog(MappingTableDataTypeTest.class);

  private static HBaseTestingUtility UTIL = null;
  private static String URL = null;
  private static HBaseAdmin admin = null;

  @BeforeClass
  public static void before() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    setUpConfigForMiniCluster(conf);
    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster(1);
    String clientPort = UTIL.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
    URL =
        JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
            + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    driver = initAndRegisterDriver(URL, ReadOnlyProps.EMPTY_PROPS);
    admin = new HBaseAdmin(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMappingHbaseTableToPhoenixTable() throws Exception {
    final byte[] tableName = Bytes.toBytes("MTEST");
    // Create table then get the single region for our new table.
    HTable t = UTIL.createTable(tableName, Bytes.toBytes("cf"));
    insertData(tableName, admin, t);
    t.close();
    try {
      testCreateTableMismatchedType();
    } catch (IllegalDataException e) {
    }
  }

  private void insertData(final byte[] tableName, HBaseAdmin admin, HTable t) throws IOException,
      InterruptedException {
    Put p = new Put(Bytes.toBytes("row"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("value1"));
    t.put(p);
    t.flushCommits();
    admin.flush(tableName);
  }

  /**
   * Test create a table in Phoenix with mismatched data type UNSIGNED_LONG
   */
  private void testCreateTableMismatchedType() throws Exception {
    String ddl =
        "create table IF NOT EXISTS MTEST (" + " id varchar NOT NULL primary key,"
            + " \"cf\".\"q1\" unsigned_long" + " ) ";
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    Connection conn = driver.connect(URL, props);
    conn.createStatement().execute(ddl);
    conn.commit();
    String query = "select * from MTEST";
    ResultSet rs = conn.createStatement().executeQuery(query);
    rs.next();
    rs.getLong(2);
  }

}


