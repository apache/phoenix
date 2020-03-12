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

import org.apache.commons.cli.CommandLine;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.phoenix.mapreduce.index.IndexTool.INVALID_TIME_RANGE_EXCEPTION_MESSAGE;

public class IndexToolTest extends BaseTest {

    IndexTool it;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

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

    @Test(timeout = 10000 /* 10 secs */)
    public void testParseOptions_timeRange_startTimeInFuture() {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        Long endTime = EnvironmentEdgeManager.currentTimeMillis();
        String [] args =
                IndexToolIT.getArgValues(true, true, generateUniqueName(),
                        generateUniqueName(), generateUniqueName(), generateUniqueName(),
                        IndexTool.IndexVerifyType.NONE, startTime, endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testParseOptions_timeRange_startTimeEqEndTime() {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Long endTime = startTime;
        String [] args =
                IndexToolIT.getArgValues(true, true, generateUniqueName(),
                        generateUniqueName(), generateUniqueName(), generateUniqueName(),
                        IndexTool.IndexVerifyType.NONE, startTime, endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testParseOptions_timeRange_startTimeGtEndTime() {
        Long startTime = 10L;
        Long endTime = 1L;
        String [] args =
                IndexToolIT.getArgValues(true, true, generateUniqueName(),
                        generateUniqueName(), generateUniqueName(), generateUniqueName(),
                        IndexTool.IndexVerifyType.NONE, startTime, endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }
}
