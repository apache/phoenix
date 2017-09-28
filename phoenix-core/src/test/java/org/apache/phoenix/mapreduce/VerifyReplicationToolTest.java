/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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
package org.apache.phoenix.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VerifyReplicationToolTest {

    private static final String SOURCE_ZK = "source-zk";
    private static final String SOURCE_TABLE = "SOURCE.TABLE";
    private static final String TARGET_ZK = "target-zk";
    private static final String TARGET_TABLE = "TARGET.TABLE";
    private static final String CONDITIONS = "TENANT_ID = 'a'";

    private VerifyReplicationTool tool;
    private VerifyReplicationTool.Builder builder;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        Configuration conf = HBaseConfiguration.create();
        tool = new VerifyReplicationTool(conf);
        builder = VerifyReplicationTool.newBuilder(conf);
    }

    @Test
    public void testParseTargetTableOptions() throws IOException {
        String[] opts = new String[] { "-t", SOURCE_TABLE, "-tt", TARGET_TABLE };
        assertTrue(tool.parseCommandLine(opts));
        assertEquals(SOURCE_TABLE, tool.getTableName());
        assertEquals(TARGET_TABLE, tool.getTargetTableName());
        assertNull(CONDITIONS, tool.getSqlConditions());
    }

    @Test
    public void testParseTargetClusterOptions() throws IOException {
        String[] opts =
                new String[] { "-t", SOURCE_TABLE, "-tz", TARGET_ZK };
        assertTrue(tool.parseCommandLine(opts));
        assertEquals("localhost", tool.getZkQuorum());
        assertEquals(SOURCE_TABLE, tool.getTableName());
        assertEquals(TARGET_ZK, tool.getTargetZkQuorum());
        assertNull(tool.getSqlConditions());
    }

    @Test
    public void testParseConditionsOptions() throws IOException {
        String[] opts =
                new String[] { "-t", SOURCE_TABLE, "-tt", TARGET_TABLE, "-c", CONDITIONS };
        assertTrue(tool.parseCommandLine(opts));
        assertEquals(CONDITIONS, tool.getSqlConditions());
    }

    @Test
    public void testParseMissingTableOption() throws IOException {
        String[] opts = new String[] {};
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("table is a required parameter");
        tool.parseCommandLine(opts);
    }

    @Test
    public void testParseTargetTableAndClusterMissing() throws IOException {
        String[] opts = new String[] {"-t", SOURCE_TABLE};
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Target table or target ZK quorum required");
        tool.parseCommandLine(opts);
    }

    @Test
    public void testParseExtraArguments() throws IOException {
        String[] opts = new String[] {"-t", SOURCE_TABLE, "-tt", TARGET_TABLE, "foo", "bar"};
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Unexpected extra parameters");
        tool.parseCommandLine(opts);
    }

    @Test
    public void testBuilderWithTargetTable() {
        builder.tableName(SOURCE_TABLE).targetTableName(TARGET_TABLE);
        tool = builder.build();
        assertEquals("localhost", tool.getZkQuorum());
        assertEquals(SOURCE_TABLE, tool.getTableName());
        assertEquals(TARGET_TABLE, tool.getTargetTableName());
        assertNull(tool.getSqlConditions());
    }

    @Test
    public void testBuilderWithTargetCluster() {
        builder.tableName(SOURCE_TABLE).zkQuorum(SOURCE_ZK).targetZkQuorum(TARGET_ZK)
                .sqlConditions(CONDITIONS);
        tool = builder.build();
        assertEquals(SOURCE_ZK, tool.getZkQuorum());
        assertEquals(SOURCE_TABLE, tool.getTableName());
        assertEquals(TARGET_ZK, tool.getTargetZkQuorum());
        assertEquals(CONDITIONS, tool.getSqlConditions());
    }

    @Test
    public void testBuilderWithMissingTable() {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("table name must be specified");
        tool = builder.build();
    }

    @Test
    public void testBuilderWithMissingTargetTableAndCluster() {
        builder.tableName(SOURCE_TABLE);
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("target table name or ZooKeeper quorum must be specified");
        tool = builder.build();
    }
}
