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
package org.apache.phoenix.tools;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PhckSystemLevelToolTest {

    @Test(expected = Exception.class)
    public void testNoParam() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {});
    }

    @Test(expected = Exception.class)
    public void testWrong() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-dummy"});
    }

    @Test
    public void testMonitorModeWithNoTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-m"});
        assertFalse(tool.isFixTableNotExistMode());
        assertTrue(tool.isMonitorMode());
        assertFalse(tool.isFixTableDisabledMode());
        assertFalse(tool.isFixMismatchedRowCountMode());
    }

    @Test
    public void testMonitorModeWithTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-m", "dummy"});
        assertFalse(tool.isFixTableNotExistMode());
        assertTrue(tool.isMonitorMode());
        assertFalse(tool.isFixTableDisabledMode());
        assertFalse(tool.isFixMismatchedRowCountMode());

        tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"--monitor", "dummy2"});
        assertFalse(tool.isFixTableNotExistMode());
        assertTrue(tool.isMonitorMode());
        assertFalse(tool.isFixTableDisabledMode());
        assertFalse(tool.isFixMismatchedRowCountMode());
    }

    @Test(expected = Exception.class)
    public void testFixMissingTableWithNoTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-f"});
    }

    @Test
    public void testFixMissingTableWithTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-f", "dummy"});
        assertTrue(tool.isFixTableNotExistMode());
        assertFalse(tool.isMonitorMode());
        assertFalse(tool.isFixTableDisabledMode());
        assertFalse(tool.isFixMismatchedRowCountMode());

        PhckSystemLevelTool tool2 = new PhckSystemLevelTool();
        tool2.parserParam(new String[] {"--fixMissingTable", "dummy2"});
        assertTrue(tool2.isFixTableNotExistMode());
        assertFalse(tool2.isMonitorMode());
        assertFalse(tool2.isFixTableDisabledMode());
        assertFalse(tool2.isFixMismatchedRowCountMode());
    }

    @Test(expected = Exception.class)
    public void testEnableTableWithNoTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-e"});
    }

    @Test
    public void testEnableTableWithTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-e", "dummy"});
        assertFalse(tool.isFixTableNotExistMode());
        assertFalse(tool.isMonitorMode());
        assertTrue(tool.isFixTableDisabledMode());
        assertFalse(tool.isFixMismatchedRowCountMode());

        PhckSystemLevelTool tool2 = new PhckSystemLevelTool();
        tool2.parserParam(new String[] {"--enableTable", "dummy2"});
        assertFalse(tool2.isFixTableNotExistMode());
        assertFalse(tool2.isMonitorMode());
        assertTrue(tool2.isFixTableDisabledMode());
        assertFalse(tool2.isFixMismatchedRowCountMode());
    }

    @Test(expected = Exception.class)
    public void testFixRowCountWithNoTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-c"});
    }

    @Test
    public void testFixRowCountWithTableName() throws Exception {
        PhckSystemLevelTool tool = new PhckSystemLevelTool();
        tool.parserParam(new String[] {"-c", "dummy"});
        assertFalse(tool.isFixTableNotExistMode());
        assertFalse(tool.isMonitorMode());
        assertFalse(tool.isFixTableDisabledMode());
        assertTrue(tool.isFixMismatchedRowCountMode());

        PhckSystemLevelTool tool2 = new PhckSystemLevelTool();
        tool2.parserParam(new String[] {"--fixRowCount", "dummy2"});
        assertFalse(tool2.isFixTableNotExistMode());
        assertFalse(tool2.isMonitorMode());
        assertFalse(tool2.isFixTableDisabledMode());
        assertTrue(tool2.isFixMismatchedRowCountMode());
    }
}
