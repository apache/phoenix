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
package org.apache.phoenix.schema.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UpdateStatisticsToolTest {

    @Test (expected = IllegalStateException.class)
    public void testTableNameIsMandatory() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {});
    }

    @Test (expected = IllegalStateException.class)
    public void testManageSnapshotAndRunFgOption1() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {"-t", "table1", "-ms"});
    }

    @Test
    public void testManageSnapshotAndRunFgOption2() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        try {
            tool.parseOptions(new String[] {"-t", "table1", "-ms", "-runfg"});
        } catch (IllegalStateException e) {
            fail("IllegalStateException is not expected " +
                    "since all required parameters are provided.");
        }
    }

    @Test
    public void testSnapshotNameInput() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg", "-s", "snap1"});
        assertEquals("snap1", tool.getSnapshotName());
    }

    @Test
    public void testSnapshotNameDefault() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg"});
        assertTrue(tool.getSnapshotName().startsWith("UpdateStatisticsTool_table1_"));
    }

    @Test
    public void testRestoreDirDefault() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg"});
        assertEquals("file:/tmp", tool.getRestoreDir().toString());
    }

    @Test
    public void testRestoreDirInput() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-d", "fs:/path"});
        assertEquals("fs:/path", tool.getRestoreDir().toString());
    }

    @Test
    public void testRestoreDirFromConfig() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(FS_DEFAULT_NAME_KEY, "hdfs://base-dir");
        tool.setConf(configuration);
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg"});
        assertEquals("hdfs://base-dir/tmp", tool.getRestoreDir().toString());
    }

    @Test
    public void testJobPriorityInput() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1"});
        assertEquals("NORMAL", tool.getJobPriority());

        tool.parseArgs(new String[] {"-t", "table1", "-p", "0"});
        assertEquals("VERY_HIGH", tool.getJobPriority());

        tool.parseArgs(new String[] {"-t", "table1", "-p", "-1"});
        assertEquals("NORMAL", tool.getJobPriority());

        tool.parseArgs(new String[] {"-t", "table1", "-p", "DSAFDAS"});
        assertEquals("NORMAL", tool.getJobPriority());
    }
}