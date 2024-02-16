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
package org.apache.phoenix.index;

import static org.apache.phoenix.mapreduce.index.IndexUpgradeTool.ROLLBACK_OP;
import static org.apache.phoenix.mapreduce.index.IndexUpgradeTool.UPGRADE_OP;
import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.ConnectionlessTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class IndexUpgradeToolTest extends BaseConnectionlessQueryTest{
    private static final String INPUT_LIST = "TEST.MOCK1,TEST1.MOCK2,TEST.MOCK3";
    private final boolean upgrade;
    private static final String DUMMY_STRING_VALUE = "anyValue";
    private static final String DUMMY_VERIFY_VALUE = "someVerifyValue";
    private static final String ONLY_VERIFY_VALUE = "ONLY";
    private IndexUpgradeTool indexUpgradeTool=null;
    private String outputFile;

    public IndexUpgradeToolTest(boolean upgrade) {
        this.upgrade = upgrade;
        this.outputFile = "/tmp/index_upgrade_" + UUID.randomUUID().toString();
    }

    private void setup(String[] args) {
        indexUpgradeTool = new IndexUpgradeTool();
        CommandLine cmd = indexUpgradeTool.parseOptions(args);
        indexUpgradeTool.initializeTool(cmd);
    }

    @Test
    public void testCommandLineParsing() {
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb",
                INPUT_LIST, "-lf", outputFile, "-d"};
        setup(args);
        Assert.assertEquals(indexUpgradeTool.getDryRun(),true);
        Assert.assertEquals(indexUpgradeTool.getInputTables(), INPUT_LIST);
        Assert.assertEquals(indexUpgradeTool.getOperation(), upgrade ? UPGRADE_OP : ROLLBACK_OP);
        Assert.assertEquals(indexUpgradeTool.getLogFile(), outputFile);
        // verify index rebuild is disabled by default
        Assert.assertEquals(false, indexUpgradeTool.getIsRebuild());
        Assert.assertNull(indexUpgradeTool.getIndexToolOpts());
    }

    @Test
    public void testRebuildOptionParsing() {
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb",
                INPUT_LIST, "-rb"};
        setup(args);
        Assert.assertEquals(true, indexUpgradeTool.getIsRebuild());
        Assert.assertNull(indexUpgradeTool.getIndexToolOpts());
    }

    @Test(expected = IllegalStateException.class)
    public void testIndexToolOptionsNoRebuild() {
        String indexToolOpts = "-v " + DUMMY_VERIFY_VALUE;
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb", INPUT_LIST,
                "-tool", indexToolOpts};
        setup(args);
    }

    @Test
    public void testIfOptionsArePassedToIndexTool() throws Exception {
        if (!upgrade) {
            return;
        }
        String [] indexToolOpts = {"-v", ONLY_VERIFY_VALUE, "-runfg", "-st", "100"};
        String indexToolarg = String.join(" ", indexToolOpts);
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb",
                INPUT_LIST, "-lf", outputFile, "-d", "-rb", "-tool", indexToolarg };
        setup(args);

        Assert.assertEquals("value passed to index tool option does not match with provided value",
                indexToolarg, indexUpgradeTool.getIndexToolOpts());
        String [] values = indexUpgradeTool.getIndexToolArgValues(DUMMY_STRING_VALUE,
                DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE);
        List<String> argList =  Arrays.asList(values);
        Assert.assertTrue(argList.contains("-v"));
        Assert.assertTrue(argList.contains(ONLY_VERIFY_VALUE));
        Assert.assertEquals("verify option and value are not passed consecutively", 1,
                argList.indexOf(ONLY_VERIFY_VALUE) - argList.indexOf("-v"));
        Assert.assertTrue(argList.contains("-runfg"));
        Assert.assertTrue(argList.contains("-st"));

        // ensure that index tool can parse the options and raises no exceptions
        IndexTool it = new IndexTool();
        CommandLine commandLine = it.parseOptions(values);
        it.populateIndexToolAttributes(commandLine);
    }

    @Test
    public void testMalformedSpacingOptionsArePassedToIndexTool() throws Exception {
        if (!upgrade) {
            return;
        }
        String [] indexToolOpts = {"-v"+ONLY_VERIFY_VALUE, "     -runfg", " -st  ", "100  "};
        String indexToolarg = String.join(" ", indexToolOpts);
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb",
                INPUT_LIST, "-rb", "-tool", indexToolarg };
        setup(args);

        Assert.assertEquals("value passed to index tool option does not match with provided value",
                indexToolarg, indexUpgradeTool.getIndexToolOpts());
        String [] values = indexUpgradeTool.getIndexToolArgValues(DUMMY_STRING_VALUE,
                DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE);
        List<String> argList =  Arrays.asList(values);
        Assert.assertTrue(argList.contains("-v" + ONLY_VERIFY_VALUE));
        Assert.assertTrue(argList.contains("-runfg"));
        Assert.assertTrue(argList.contains("-st"));

        // ensure that index tool can parse the options and raises no exceptions
        IndexTool it = new IndexTool();
        CommandLine commandLine = it.parseOptions(values);
        it.populateIndexToolAttributes(commandLine);
    }

    @Test(expected = IllegalStateException.class)
    public void testBadIndexToolOptions() throws Exception {
        String [] indexToolOpts = {"-v" + DUMMY_VERIFY_VALUE};
        String indexToolarg = String.join(" ", indexToolOpts);
        String [] args = {"-o", UPGRADE_OP, "-tb", INPUT_LIST, "-rb", "-tool", indexToolarg };
        setup(args);
        String [] values = indexUpgradeTool.getIndexToolArgValues(DUMMY_STRING_VALUE,
                DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE);
        IndexTool it = new IndexTool();
        CommandLine commandLine = it.parseOptions(values);
        it.populateIndexToolAttributes(commandLine);
    }

    @Parameters(name ="IndexUpgradeToolTest_mutable={1}")
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList( false, true);
    }

    private void setupConfForConnectionlessQuery(Configuration conf) {
        String connectionlessUrl = PhoenixRuntime.JDBC_PROTOCOL_ZK + JDBC_PROTOCOL_SEPARATOR
        + CONNECTIONLESS + JDBC_PROTOCOL_TERMINATOR
        + PHOENIX_TEST_DRIVER_URL_PARAM + JDBC_PROTOCOL_TERMINATOR;
        PhoenixConfigurationUtil.setInputClusterUrl(conf, connectionlessUrl);
        PhoenixConfigurationUtil.setOutputClusterUrl(conf, connectionlessUrl);
        conf.unset(HConstants.ZOOKEEPER_CLIENT_PORT);
        conf.unset(HConstants.ZOOKEEPER_ZNODE_PARENT);
    }

    @Test
    public void testConnectionProperties() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        long indexRebuildQueryTimeoutMs = 2000;
        long indexRebuildRpcTimeoutMs = 3000;
        long indexRebuildClientScannerTimeoutMs = 4000;
        int indexRebuildRpcRetryCount = 10;

        conf.setLong(QueryServices.INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB, indexRebuildQueryTimeoutMs);
        conf.setLong(QueryServices.INDEX_REBUILD_RPC_TIMEOUT_ATTRIB, indexRebuildRpcTimeoutMs);
        conf.setLong(QueryServices.INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB,
                indexRebuildClientScannerTimeoutMs);
        conf.setInt(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER, indexRebuildRpcRetryCount);

        // prepare conf for connectionless query
        setupConfForConnectionlessQuery(conf);

        try (Connection conn = IndexUpgradeTool.getConnection(conf)) {
            // verify connection properties for phoenix, hbase timeouts and retries
            Assert.assertEquals(conn.getClientInfo(QueryServices.THREAD_TIMEOUT_MS_ATTRIB),
                    Long.toString(indexRebuildQueryTimeoutMs));
            Assert.assertEquals(conn.getClientInfo(HConstants.HBASE_RPC_TIMEOUT_KEY),
                    Long.toString(indexRebuildRpcTimeoutMs));
            Assert.assertEquals(conn.getClientInfo(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD),
                    Long.toString(indexRebuildClientScannerTimeoutMs));
            Assert.assertEquals(conn.getClientInfo(HConstants.HBASE_CLIENT_RETRIES_NUMBER),
                    Long.toString(indexRebuildRpcRetryCount));
        }
    }

    @Test
    public void testConnectionDefaults() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        long indexRebuildQueryTimeoutMs = conf.getLong(
                QueryServices.INDEX_REBUILD_QUERY_TIMEOUT_ATTRIB,
                QueryServicesOptions.DEFAULT_INDEX_REBUILD_QUERY_TIMEOUT);
        long indexRebuildRpcTimeoutMs = conf.getLong(
                QueryServices.INDEX_REBUILD_RPC_TIMEOUT_ATTRIB,
                QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_TIMEOUT);
        long indexRebuildClientScannerTimeoutMs = conf.getLong(
                QueryServices.INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT_ATTRIB,
                QueryServicesOptions.DEFAULT_INDEX_REBUILD_CLIENT_SCANNER_TIMEOUT);
        long indexRebuildRpcRetryCount = conf.getInt(
                QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER,
                QueryServicesOptions.DEFAULT_INDEX_REBUILD_RPC_RETRIES_COUNTER);

        // prepare conf for connectionless query
        setupConfForConnectionlessQuery(conf);

        try (Connection conn = IndexUpgradeTool.getConnection(conf)) {
            // verify connection properties for phoenix, hbase timeouts and retries
            Assert.assertEquals(conn.getClientInfo(QueryServices.THREAD_TIMEOUT_MS_ATTRIB),
                    Long.toString(indexRebuildQueryTimeoutMs));
            Assert.assertEquals(conn.getClientInfo(HConstants.HBASE_RPC_TIMEOUT_KEY),
                    Long.toString(indexRebuildRpcTimeoutMs));
            Assert.assertEquals(conn.getClientInfo(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD),
                    Long.toString(indexRebuildClientScannerTimeoutMs));
            Assert.assertEquals(conn.getClientInfo(HConstants.HBASE_CLIENT_RETRIES_NUMBER),
                    Long.toString(indexRebuildRpcRetryCount));
        }
    }
}
