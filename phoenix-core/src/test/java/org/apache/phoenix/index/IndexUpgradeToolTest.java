package org.apache.phoenix.index;

import static org.apache.phoenix.mapreduce.index.IndexUpgradeTool.ROLLBACK_OP;
import static org.apache.phoenix.mapreduce.index.IndexUpgradeTool.UPGRADE_OP;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class IndexUpgradeToolTest {
    private static final String INPUT_LIST = "TEST.MOCK1,TEST1.MOCK2,TEST.MOCK3";
    private final boolean upgrade;

    public IndexUpgradeToolTest(boolean upgrade) {
        this.upgrade = upgrade;
    }

    @Test
    public void testCommandLineParsing() {

        String outputFile = "/tmp/index_upgrade_" + UUID.randomUUID().toString();
        String [] args = {"-o", upgrade ? UPGRADE_OP : ROLLBACK_OP, "-tb",
                INPUT_LIST, "-lf", outputFile, "-d"};
        IndexUpgradeTool iut = new IndexUpgradeTool();

        CommandLine cmd = iut.parseOptions(args);
        iut.initializeTool(cmd);
        Assert.assertEquals(iut.getDryRun(),true);
        Assert.assertEquals(iut.getInputTables(), INPUT_LIST);
        Assert.assertEquals(iut.getOperation(), upgrade ? UPGRADE_OP : ROLLBACK_OP);
        Assert.assertEquals(iut.getLogFile(), outputFile);
    }

    @Parameters(name ="IndexUpgradeToolTest_mutable={1}")
    public static Collection<Boolean> data() {
        return Arrays.asList( false, true);
    }

}
