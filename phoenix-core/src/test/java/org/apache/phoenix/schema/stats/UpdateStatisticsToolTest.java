package org.apache.phoenix.schema.stats;

import org.junit.Test;

public class UpdateStatisticsToolTest {

    @Test (expected = IllegalStateException.class)
    public void testTableNameIsMandatory() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {});
    }

    @Test
    public void testCreateSnapshotAndSnapshotNameOption1() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {"-t", "table1", "-s", "snap1"});
    }

    @Test (expected = IllegalStateException.class)
    public void testCreateSnapshotAndSnapshotNameOption2() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {"-t", "table1"});
    }

    @Test
    public void testCreateSnapshotAndSnapshotNameOption3() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {"-t", "table1", "-cs"});
    }

    @Test (expected = IllegalStateException.class)
    public void testDeleteSnapshotAndRunFgOption1() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {"-t", "table1", "-cs", "-ds"});
    }

    @Test
    public void testDeleteSnapshotAndRunFgOption2() {
        UpdateStatisticsTool tool = new UpdateStatisticsTool();
        tool.parseOptions(new String[] {"-t", "table1", "-cs", "-ds", "-runfg"});
    }

}