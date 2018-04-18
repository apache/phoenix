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

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestProcessExecResult;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.google.common.base.Throwables;

/**
 * Base class for all Hive Phoenix integration tests that may be run with Tez or MR mini cluster
 */
@Category(NeedsOwnMiniClusterTest.class)
@Ignore
public class BaseHivePhoenixStoreIT {

    private static final Log LOG = LogFactory.getLog(BaseHivePhoenixStoreIT.class);
    protected static HBaseTestingUtility hbaseTestUtil;
    protected static MiniHBaseCluster hbaseCluster;
    private static String zkQuorum;
    protected static Connection conn;
    private static Configuration conf;
    protected static HiveTestUtil qt;
    protected static String hiveOutputDir;
    protected static String hiveLogDir;


    public static void setup(HiveTestUtil.MiniClusterType clusterType)throws Exception {
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (null != hadoopConfDir && !hadoopConfDir.isEmpty()) {
          LOG.warn("WARNING: HADOOP_CONF_DIR is set in the environment which may cause "
              + "issues with test execution via MiniDFSCluster");
        }
        hbaseTestUtil = new HBaseTestingUtility();
        conf = hbaseTestUtil.getConfiguration();
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        conf.set("hive.metastore.schema.verification","false");
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
        System.setProperty(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString(), "false");
        //System.setProperty(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL.toString(),"true");
        try {
            qt = new HiveTestUtil(hiveOutputDir, hiveLogDir, clusterType, "", "0.20",null, null, false);
            // do a one time initialization
            qt.createSources();
        } catch (Exception e) {
            LOG.error("Unexpected exception in setup: " + e.getMessage(), e);
            fail("Unexpected exception in setup"+Throwables.getStackTraceAsString(e));
        }

        //Start HBase cluster
        hbaseCluster = hbaseTestUtil.startMiniCluster(1);
        //MiniDFSCluster x = hbaseTestUtil.getDFSCluster();
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

    protected void runTest(String fname, String fpath) throws Exception {
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
                return;
            }

            QTestProcessExecResult result = qt.checkCliDriverResults(fname);
            if (result.getReturnCode() != 0) {
              qt.failedDiff(result.getReturnCode(), fname, result.getCapturedOutput());
            }
            qt.clearPostTestEffects();

        } catch (Throwable e) {
            qt.failed(new Exception(e), fname, null);
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        LOG.info("Done query: " + fname + " elapsedTime=" + elapsedTime / 1000 + "s");
        assertTrue("Test passed", true);
    }

    protected void createFile(String content, String fullName) throws IOException {
        FileUtils.write(new File(fullName), content);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
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
        // Shutdowns down the filesystem -- do this after stopping HBase.
        if (qt != null) {
          try {
              qt.shutdown();
          } catch (Exception e) {
              LOG.error("Unexpected exception in setup", e);
              //fail("Unexpected exception in tearDown");
          }
      }
    }
}
