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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.phoenix.mapreduce.index.IndexUpgradeTool;
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

}
