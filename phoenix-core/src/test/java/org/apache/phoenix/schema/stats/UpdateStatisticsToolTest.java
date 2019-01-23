package org.apache.phoenix.schema.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

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
        tool.parseOptions(new String[] {"-t", "table1", "-ms", "-runfg"});
    }

    @Test
    public void testSnapshotNameInput() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg", "-s", "snap1"});
        Assert.assertEquals("snap1", tool.getSnapshotName());
    }

    @Test
    public void testSnapshotNameDefault() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg"});
        Assert.assertTrue(tool.getSnapshotName().startsWith("UpdateStatisticsTool_table1_"));
    }

    @Test
    public void testRestoreDirDefault() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg"});
        Assert.assertEquals("file:/tmp", tool.getRestoreDir().toString());
    }

    @Test
    public void testRestoreDirInput() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseArgs(new String[] {"-t", "table1", "-d", "fs:/path"});
        Assert.assertEquals("fs:/path", tool.getRestoreDir().toString());
    }

    @Test
    public void testRestoreDirFromConfig() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(FS_DEFAULT_NAME_KEY, "hdfs://base-dir");
        tool.setConf(configuration);
        tool.parseArgs(new String[] {"-t", "table1", "-ms", "-runfg"});
        Assert.assertEquals("hdfs://base-dir/tmp", tool.getRestoreDir().toString());
    }

}