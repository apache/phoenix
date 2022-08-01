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

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.phoenix.mapreduce.index.IndexTool.FEATURE_NOT_APPLICABLE;
import static org.apache.phoenix.mapreduce.index.IndexTool.INVALID_TIME_RANGE_EXCEPTION_MESSAGE;
import static org.apache.phoenix.mapreduce.index.IndexTool.RETRY_VERIFY_NOT_APPLICABLE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class IndexToolTest extends BaseTest {

    IndexTool it;
    private String dataTable;
    private String indexTable;
    private String schema;
    private String tenantId;
    @Mock
    PTable pDataTable;
    boolean localIndex = true;

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        it = new IndexTool();
        schema = generateUniqueName();
        dataTable = generateUniqueName();
        indexTable = generateUniqueName();
        tenantId = generateUniqueName();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testParseOptions_timeRange_timeRangeNotNull() throws Exception {
        Long startTime = 10L;
        Long endTime = 15L;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(startTime, it.getStartTime());
        assertEquals(endTime, it.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_null() throws Exception {
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        Assert.assertNull(it.getStartTime());
        Assert.assertNull(it.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_startTimeNotNull() throws Exception {
        Long startTime = 10L;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , null);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(startTime, it.getStartTime());
        assertEquals(null, it.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_endTimeNotNull() throws Exception {
        Long endTime = 15L;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        null , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(null, it.getStartTime());
        assertEquals(endTime, it.getEndTime());
    }

    @Test
    public void testParseOptions_timeRange_startTimeNullEndTimeInFuture() throws Exception {
        Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        null , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testParseOptions_timeRange_endTimeNullStartTimeInFuture() throws Exception {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , null);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test(timeout = 10000 /* 10 secs */)
    public void testParseOptions_timeRange_startTimeInFuture() throws Exception {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 200000;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test(timeout = 10000 /* 10 secs */)
    public void testParseOptions_timeRange_endTimeInFuture() throws Exception {
        Long startTime = EnvironmentEdgeManager.currentTimeMillis();
        Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testParseOptions_timeRange_startTimeEqEndTime() throws Exception {
        Long startTime = 10L;
        Long endTime = 10L;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testParseOptions_timeRange_startTimeGtEndTime() throws Exception {
        Long startTime = 10L;
        Long endTime = 1L;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
        it.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testCheckTimeRangeFeature_timeRangeSet_transactionalTable_globalIndex() {
        when(pDataTable.isTransactional()).thenReturn(true);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(FEATURE_NOT_APPLICABLE);
        IndexTool.checkIfFeatureApplicable(1L, 3L, null, pDataTable, !localIndex);
    }

    @Test
    public void testIncrcementalVerifyOption() throws Exception {
        IndexTool mockTool = Mockito.mock(IndexTool.class);
        when(mockTool.getLastVerifyTime()).thenCallRealMethod();
        Long lastVerifyTime = 10L;
        String [] args =
                IndexToolIT.getArgValues(true, schema,
                        dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                        lastVerifyTime, null, IndexTool.IndexDisableLoggingType.NONE, lastVerifyTime);
        when(mockTool.parseOptions(args)).thenCallRealMethod();

        CommandLine cmdLine = mockTool.parseOptions(args);

        when(mockTool.populateIndexToolAttributes(cmdLine)).thenCallRealMethod();
        when(mockTool.isValidLastVerifyTime(lastVerifyTime)).thenReturn(true);

        mockTool.populateIndexToolAttributes(cmdLine);
        Assert.assertEquals(lastVerifyTime, mockTool.getLastVerifyTime());

        when(pDataTable.isTransactional()).thenReturn(true);
        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(FEATURE_NOT_APPLICABLE);
        IndexTool.checkIfFeatureApplicable(null, null, lastVerifyTime, pDataTable, !localIndex);
    }

    @Test
    public void testIncrcementalVerifyOption_notApplicable() throws Exception {
        IndexTool mockTool = Mockito.mock(IndexTool.class);
        when(mockTool.getLastVerifyTime()).thenCallRealMethod();
        Long lastVerifyTime = 10L;
        String [] args =
            IndexToolIT.getArgValues(true, schema,
                dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.AFTER,
                lastVerifyTime, null, IndexTool.IndexDisableLoggingType.NONE,
                lastVerifyTime);
        when(mockTool.parseOptions(args)).thenCallRealMethod();

        CommandLine cmdLine = mockTool.parseOptions(args);

        when(mockTool.populateIndexToolAttributes(cmdLine)).thenCallRealMethod();
        when(mockTool.validateLastVerifyTime()).thenCallRealMethod();
        when(mockTool.isValidLastVerifyTime(lastVerifyTime)).thenReturn(false);

        exceptionRule.expect(RuntimeException.class);
        exceptionRule.expectMessage(RETRY_VERIFY_NOT_APPLICABLE);
        mockTool.populateIndexToolAttributes(cmdLine);
    }

    @Test
    public void testCheckVerifyAndDisableLogging_defaultsNone() throws Exception {
        Long startTime = null;
        Long endTime = 10L;
        String [] args =
            IndexToolIT.getArgValues(true, schema,
                dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(IndexTool.IndexDisableLoggingType.NONE, it.getDisableLoggingType());
    }

    @Test
    public void testDisableLogging_allowsNone() throws Exception {
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.NONE, IndexTool.IndexVerifyType.NONE);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.NONE, IndexTool.IndexVerifyType.ONLY);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.NONE, IndexTool.IndexVerifyType.BEFORE);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.NONE, IndexTool.IndexVerifyType.AFTER);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.NONE, IndexTool.IndexVerifyType.BOTH);
    }

    @Test
    public void testDisableLogging_allowsBefore() throws Exception {
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.BEFORE, IndexTool.IndexVerifyType.BEFORE);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.BEFORE, IndexTool.IndexVerifyType.ONLY);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.BEFORE, IndexTool.IndexVerifyType.BOTH);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BEFORE,
            IndexTool.IndexVerifyType.AFTER);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BEFORE,
            IndexTool.IndexVerifyType.NONE);
    }

    @Test
    public void testDisableLogging_allowsAfter() throws Exception {
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.AFTER, IndexTool.IndexVerifyType.BOTH);
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.AFTER, IndexTool.IndexVerifyType.AFTER);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.AFTER,
            IndexTool.IndexVerifyType.NONE);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.AFTER,
            IndexTool.IndexVerifyType.BEFORE);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BOTH,
            IndexTool.IndexVerifyType.ONLY);
    }

    @Test
    public void testCheckVerifyAndDisableLogging_allowsBoth() throws Exception {
        verifyDisableLogging(IndexTool.IndexDisableLoggingType.BOTH, IndexTool.IndexVerifyType.BOTH);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BOTH,
            IndexTool.IndexVerifyType.NONE);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BOTH,
            IndexTool.IndexVerifyType.ONLY);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BOTH,
            IndexTool.IndexVerifyType.BEFORE);
        verifyDisableLoggingException(IndexTool.IndexDisableLoggingType.BOTH,
            IndexTool.IndexVerifyType.AFTER);
    }

    public void verifyDisableLogging(IndexTool.IndexDisableLoggingType disableType,
                                     IndexTool.IndexVerifyType verifyType) throws Exception {
        Long startTime = null;
        Long endTime = 10L;
        String[] args =
            IndexToolIT.getArgValues(true, schema,
                dataTable, indexTable, tenantId, verifyType,
                startTime, endTime, disableType, null);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(disableType, it.getDisableLoggingType());
    }

    public void verifyDisableLoggingException(IndexTool.IndexDisableLoggingType disableType,
                                     IndexTool.IndexVerifyType verifyType) {
        Long startTime = null;
        Long endTime = 10L;
        String[] args =
            IndexToolIT.getArgValues(true, schema,
                dataTable, indexTable, tenantId, verifyType,
                startTime, endTime, disableType, null);
        exceptionRule.expect(IllegalStateException.class);
        CommandLine cmdLine = it.parseOptions(args);
    }

    @Test
    public void testIndexToolDefaultSource() throws Exception {
        Long startTime = 1L;
        Long endTime = 10L;
        String [] args =
            IndexToolIT.getArgValues(true, schema,
                dataTable, indexTable, tenantId, IndexTool.IndexVerifyType.NONE,
                startTime , endTime);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE, it.getSourceTable());
    }

    @Test
    public void testIndexToolFromIndexSource() throws Exception {
        verifyFromIndexOption(IndexTool.IndexVerifyType.ONLY);
        verifyFromIndexOption(IndexTool.IndexVerifyType.BEFORE);
    }

    private void verifyFromIndexOption(IndexTool.IndexVerifyType verifyType) throws Exception {
        Long startTime = 1L;
        Long endTime = 10L;
        String[] args =
            IndexToolIT.getArgValues(true, true, schema,
                dataTable, indexTable, tenantId, verifyType,
                startTime, endTime, IndexTool.IndexDisableLoggingType.BEFORE, null, true);
        CommandLine cmdLine = it.parseOptions(args);
        it.populateIndexToolAttributes(cmdLine);
        assertEquals(IndexScrutinyTool.SourceTable.INDEX_TABLE_SOURCE, it.getSourceTable());
    }

}
