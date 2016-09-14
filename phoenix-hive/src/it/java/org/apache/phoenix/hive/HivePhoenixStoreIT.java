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
package org.apache.phoenix.hive;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test class to run all Hive Phoenix integration tests against a MINI Map-Reduce cluster.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class HivePhoenixStoreIT {

    private static final Log LOG = LogFactory.getLog(HivePhoenixStoreIT.class);
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static Connection conn;
    private static Configuration conf;
    private static HiveTestUtil qt;
    private static String hiveOutputDir;
    private static String hiveLogDir;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (null != hadoopConfDir && !hadoopConfDir.isEmpty()) {
          LOG.warn("WARNING: HADOOP_CONF_DIR is set in the environment which may cause "
              + "issues with test execution via MiniDFSCluster");
        }
        hbaseTestUtil = new HBaseTestingUtility();
        conf = hbaseTestUtil.getConfiguration();
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        hiveOutputDir = new Path(hbaseTestUtil.getDataTestDir(), "hive_output").toString();
        File outputDir = new File(hiveOutputDir);
        outputDir.mkdirs();
        hiveLogDir = new Path(hbaseTestUtil.getDataTestDir(), "hive_log").toString();
        File logDir = new File(hiveLogDir);
        logDir.mkdirs();
        // Setup Hive mini Server
        Path testRoot = hbaseTestUtil.getDataTestDir();
        System.setProperty("test.tmp.dir", testRoot.toString());
        System.setProperty("test.warehouse.dir", (new Path(testRoot, "warehouse")).toString());

        HiveTestUtil.MiniClusterType miniMR = HiveTestUtil.MiniClusterType.mr;
        try {
            qt = new HiveTestUtil(hiveOutputDir, hiveLogDir, miniMR, null);
        } catch (Exception e) {
            LOG.error("Unexpected exception in setup", e);
            fail("Unexpected exception in setup");
        }

        //Start HBase cluster
        hbaseTestUtil.startMiniCluster(3);
        MiniDFSCluster x = hbaseTestUtil.getDFSCluster();

        Class.forName(PhoenixDriver.class.getName());
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL +
                PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum, props);
        // Setup Hive Output Folder

        Statement stmt = conn.createStatement();
        stmt.execute("create table t(a integer primary key,b varchar)");
    }

    /**
     * Create a table with two column, insert 1 row, check that phoenix table is created and
     * the row is there
     *
     * @throws Exception
     */
    @Test
    public void simpleTest() throws Exception {
        String testName = "simpleTest";
        // create a dummy outfile under log folder
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE phoenix_table(ID STRING, SALARY STRING)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='phoenix_table'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost:" + hbaseTestUtil.getZkCluster()
                .getClientPort() + "', 'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE phoenix_table" + HiveTestUtil.CRLF +
                "VALUES ('10', '1000');" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_table";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 2);
        assertTrue(rs.next());
        assert (rs.getString(1).equals("10"));
        assert (rs.getString(2).equals("1000"));

    }

    /**
     * Datatype Test
     *
     * @throws Exception
     */
    @Test
    public void dataTypeTest() throws Exception {
        String testName = "dataTypeTest";
        // create a dummy outfile under log folder
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE phoenix_datatype(ID int, description STRING, ts TIMESTAMP, db " +
                "DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='phoenix_datatype'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost:" + hbaseTestUtil.getZkCluster()
                .getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE phoenix_datatype" + HiveTestUtil.CRLF +
                "VALUES (10, \"foodesc\",\"2013-01-05 01:01:01\",200,2.0,-1);" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_datatype";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 6);
        while (rs.next()) {
            assert (rs.getInt(1) == 10);
            assert (rs.getString(2).equalsIgnoreCase("foodesc"));
            /* Need a way how to correctly handle timestamp since Hive's implementation uses
            time zone information but Phoenix doesn't.
             */
            //assert(rs.getTimestamp(3).equals(Timestamp.valueOf("2013-01-05 02:01:01")));
            assert (rs.getDouble(4) == 200);
            assert (rs.getFloat(5) == 2.0);
            assert (rs.getInt(6) == -1);
        }
    }

    /**
     * Datatype Test
     *
     * @throws Exception
     */
    @Test
    public void MultiKey() throws Exception {
        String testName = "MultiKey";
        // create a dummy outfile under log folder
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE phoenix_MultiKey(ID int, ID2 String,description STRING, ts " +
                "TIMESTAMP, db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='phoenix_MultiKey'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost:" + hbaseTestUtil.getZkCluster()
                .getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');");
        sb.append("INSERT INTO TABLE phoenix_MultiKey" + HiveTestUtil.CRLF +
                "VALUES (10,  \"part2\",\"foodesc\",\"2013-01-05 01:01:01\",200,2.0,-1);" +
                HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_MultiKey";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 7);
        while (rs.next()) {
            assert (rs.getInt(1) == 10);
            assert (rs.getString(2).equalsIgnoreCase("part2"));
            assert (rs.getString(3).equalsIgnoreCase("foodesc"));
            //assert(rs.getTimestamp(4).equals(Timestamp.valueOf("2013-01-05 02:01:01")));
            assert (rs.getDouble(5) == 200);
            assert (rs.getFloat(6) == 2.0);
            assert (rs.getInt(7) == -1);
        }
    }


    private void runTest(String fname, String fpath) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            LOG.info("Begin query: " + fname);
            qt.addFile(fpath);

            if (qt.shouldBeSkipped(fname)) {
                LOG.info("Test " + fname + " skipped");
                return;
            }

            qt.cliInit(fname);
            qt.clearTestSideEffects();
            int ecode = qt.executeClient(fname);
            if (ecode != 0) {
                qt.failed(ecode, fname, null);
            }

            ecode = qt.checkCliDriverResults(fname);
            if (ecode != 0) {
                qt.failedDiff(ecode, fname, null);
            }
            qt.clearPostTestEffects();

        } catch (Throwable e) {
            qt.failed(e, fname, null);
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info("Done query: " + fname + " elapsedTime=" + elapsedTime / 1000 + "s");
        assertTrue("Test passed", true);
    }

    private void createFile(String content, String fullName) throws IOException {
        FileUtils.write(new File(fullName), content);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (qt != null) {
            try {
                qt.shutdown();
            } catch (Exception e) {
                LOG.error("Unexpected exception in setup", e);
                fail("Unexpected exception in tearDown");
            }
        }
        try {
            conn.close();
        } finally {
            try {
                PhoenixDriver.INSTANCE.close();
            } finally {
                try {
                    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
                } finally {
                    hbaseTestUtil.shutdownMiniCluster();
                }
            }
        }
    }
}
