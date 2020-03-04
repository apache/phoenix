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

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class IndexUpgradeToolTest {
    private static final String INPUT_LIST = "TEST.MOCK1,TEST1.MOCK2,TEST.MOCK3";
    private final boolean upgrade;
    private static final String DUMMY_STRING_VALUE = "anyValue";
    private static final String DUMMY_VERIFY_VALUE = "someVerifyValue";
    private IndexUpgradeTool indexUpgradeTool=null;
    private String outputFile;

    public IndexUpgradeToolTest(boolean upgrade) {
        this.upgrade = upgrade;
    }

    @Before
    public void setup() {
        outputFile = "/tmp/index_upgrade_" + UUID.randomUUID().toString();
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb",
                INPUT_LIST, "-lf", outputFile, "-d", "-v", DUMMY_VERIFY_VALUE};
        indexUpgradeTool = new IndexUpgradeTool();
        CommandLine cmd = indexUpgradeTool.parseOptions(args);
        indexUpgradeTool.initializeTool(cmd);
    }

    @Test
    public void testCommandLineParsing() {
        Assert.assertEquals(indexUpgradeTool.getDryRun(),true);
        Assert.assertEquals(indexUpgradeTool.getInputTables(), INPUT_LIST);
        Assert.assertEquals(indexUpgradeTool.getOperation(), upgrade ? UPGRADE_OP : ROLLBACK_OP);
        Assert.assertEquals(indexUpgradeTool.getLogFile(), outputFile);
    }

    @Test
    public void testIfVerifyOptionIsPassedToTool() {
        if (!upgrade) {
            return;
        }
        Assert.assertEquals("value passed with verify option does not match with provided value",
                DUMMY_VERIFY_VALUE, indexUpgradeTool.getVerify());
        String [] values = indexUpgradeTool.getIndexToolArgValues(DUMMY_STRING_VALUE,
                DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE, DUMMY_STRING_VALUE);
        List<String> argList =  Arrays.asList(values);
        Assert.assertTrue(argList.contains(DUMMY_VERIFY_VALUE));
        Assert.assertTrue(argList.contains("-v"));
        Assert.assertEquals("verify option and value are not passed consecutively", 1,
                argList.indexOf(DUMMY_VERIFY_VALUE) - argList.indexOf("-v"));
    }

    @Parameters(name ="IndexUpgradeToolTest_mutable={1}")
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList( false, true);
    }

    private void setupConfForConnectionlessQuery(Configuration conf) {
        conf.set(HConstants.ZOOKEEPER_QUORUM, PhoenixRuntime.CONNECTIONLESS);
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
