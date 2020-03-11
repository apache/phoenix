package org.apache.phoenix.index;

import org.apache.commons.cli.CommandLine;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexToolTest extends BaseTest {

    IndexTool it;

    @Before
    public void setup() {
        it = new IndexTool();
    }

    @Test
    public void testParseOptions_timeRange_notNull() {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Long endTime = startTime + 10000;
        String [] args =
                IndexToolIT.getArgValues(true, true, generateUniqueName(),
                        generateUniqueName(), generateUniqueName(), generateUniqueName(),
                        IndexTool.IndexVerifyType.NONE, startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        Assert.assertEquals(startTime, it.getStartTime());
        Assert.assertEquals(endTime, it.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_null() {
        String [] args =
                IndexToolIT.getArgValues(true, true, generateUniqueName(),
                        generateUniqueName(), generateUniqueName(), generateUniqueName(),
                        IndexTool.IndexVerifyType.NONE);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        Assert.assertNull(it.getStartTime());
        Assert.assertNull(it.getEndTime());
    }
}
