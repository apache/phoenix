/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Preconditions;

/**
 * 
 * Test class to run all Hive Phoenix integration tests against a MINI Map-Reduce cluster.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixHiveCreate {
    
    private static final Log LOG = LogFactory.getLog(PhoenixHiveCreate.class);
    private static final String SCHEMA_NAME = "T";
    private static final String TABLE_NAME = "HIVE_TEST";
    private static Path TEST_ROOT;
    private static final String TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, TABLE_NAME);
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static Connection conn;
    private static Configuration conf;
    private static HiveTestUtil qt;
    private static String hiveOutputDir;
    private static String hiveLogDir;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        conf = hbaseTestUtil.getConfiguration();
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        hbaseTestUtil.startMiniCluster(3);

        Class.forName(PhoenixDriver.class.getName());
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL +
                 PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum,props);
        
        // Setup Hive Output Folder
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
        
     }
    
	
    /**
     * Datatype Test
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
        sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS phoenix_datatype(ID int, description STRING, ts TIMESTAMP, db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                  " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" +HiveTestUtil.CRLF+
                  " TBLPROPERTIES(" + HiveTestUtil.CRLF+
                  "   'phoenix.hbase.table.name'='phoenix_datatype'," + HiveTestUtil.CRLF+
                  "   'phoenix.zookeeper.znode.parent'='hbase'," + HiveTestUtil.CRLF+
         "   'phoenix.zookeeper.quorum'='localhost:" + hbaseTestUtil.getZkCluster().getClientPort() + "'," +HiveTestUtil.CRLF+ 
                  "   'phoenix.rowkeys'='id'," + HiveTestUtil.CRLF+
                  "   'autocreate'='true'," + HiveTestUtil.CRLF+
                  "   'autodrop'='true'," + HiveTestUtil.CRLF+
                  "   'phoenix.column.mapping'='description:B.description');" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE phoenix_datatype" + HiveTestUtil.CRLF+
        		  "VALUES (10, \"foodesc\",\"2013-01-05 01:01:01\",200,2.0,-1);" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
        
        String phoenixQuery = "SELECT * FROM phoenix_datatype";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert(rs.getMetaData().getColumnCount() == 6);
        while(rs.next()){
        	assert(rs.getInt(1) == 10);
        	assert(rs.getString(2).equalsIgnoreCase("foodesc"));
        	assert(rs.getTimestamp(3).equals(Timestamp.valueOf("2013-01-05 02:01:01")));
        	assert(rs.getDouble(4) == 200);
        	assert(rs.getFloat(5) == 2.0);
        	assert(rs.getInt(6) == -1);	
        } 
    }
    
    /**
     * Datatype Test
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
        sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS phoenix_MultiKey(ID int, ID2 String,description STRING, ts TIMESTAMP, db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                  " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" +HiveTestUtil.CRLF+
                  " TBLPROPERTIES(" + HiveTestUtil.CRLF+
                  "   'phoenix.hbase.table.name'='phoenix_MultiKey'," + HiveTestUtil.CRLF+
                  "   'phoenix.zookeeper.znode.parent'='hbase'," + HiveTestUtil.CRLF+
         "   'phoenix.zookeeper.quorum'='localhost:" + hbaseTestUtil.getZkCluster().getClientPort() + "'," +HiveTestUtil.CRLF+ 
                  "   'phoenix.rowkeys'='id,id2'," + HiveTestUtil.CRLF+
                  "   'autocreate'='true'," + HiveTestUtil.CRLF+
                  "   'autodrop'='true'," + HiveTestUtil.CRLF+
                  "   'phoenix.column.mapping'='description:B.description');" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE phoenix_MultiKey" + HiveTestUtil.CRLF+
        		  "VALUES (10,  \"part2\",\"foodesc\",\"2013-01-05 01:01:01\",200,2.0,-1);" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
        
        String phoenixQuery = "SELECT * FROM phoenix_MultiKey";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert(rs.getMetaData().getColumnCount() == 7);
        while(rs.next()){
        	assert(rs.getInt(1) == 10);
        	assert(rs.getString(2).equalsIgnoreCase("part2"));
        	assert(rs.getString(3).equalsIgnoreCase("foodesc"));
        	assert(rs.getTimestamp(4).equals(Timestamp.valueOf("2013-01-05 02:01:01")));
        	assert(rs.getDouble(5) == 200);
        	assert(rs.getFloat(6) == 2.0);
        	assert(rs.getInt(7) == -1);	
        } 
    }
       
    
    private void runTest(String fname, String fpath) throws Exception {
        long startTime = System.currentTimeMillis();
        try {
            LOG.info("Begin query: " + fname);
            System.err.println("Begin query: " + fname);

            qt.addFile(fpath);
            
            if (qt.shouldBeSkipped(fname)) {
                LOG.error("Test " + fname + " skipped");
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
        System.err.println("Done query: " + fname + " elapsedTime=" + elapsedTime/1000 + "s");
        assertTrue("Test passed", true);
    }

    private void createFile(String content, String fullName) throws IOException {
        FileUtils.write(new File(fullName), content);
    }


    private void dropTable(String tableFullName) throws SQLException {
        Preconditions.checkNotNull(conn);
        conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s",tableFullName));
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    	 if(qt == null) return;
         try {
             qt.shutdown();
         }
         catch (Exception e) {
             LOG.error("Unexpected exception in setup", e);
             fail("Unexpected exception in tearDown");
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
    
    public String read(File queryFile) throws IOException {
        InputStreamReader isr = new InputStreamReader(
                new BufferedInputStream(new FileInputStream(queryFile)), HiveTestUtil.UTF_8);
        StringWriter sw = new StringWriter();
        try {
            IOUtils.copy(isr, sw);
        } finally {
            if (isr != null) {
                isr.close();
            }
        }
        return sw.toString();
    }
}