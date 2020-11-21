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
package org.apache.phoenix.mapreduce;

import org.apache.phoenix.query.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PhoenixTTLToolTest extends BaseTest {
    String viewName = generateUniqueName();
    String tenantId = generateUniqueName();

    @Test
    public void testParseInput() {
        PhoenixTTLTool tool = new PhoenixTTLTool();
        tool.parseArgs(new String[] {"-a"});

        assertEquals("NORMAL", tool.getJobPriority());
        assertEquals(true, tool.isDeletingAllViews());
        assertEquals(null, tool.getViewName());
        assertEquals(null, tool.getTenantId());

        tool = new PhoenixTTLTool();
        tool.parseArgs(new String[] {"-v", viewName, "-i",tenantId });
        assertEquals("NORMAL", tool.getJobPriority());
        assertEquals(false, tool.isDeletingAllViews());
        assertEquals(viewName, tool.getViewName());
        assertEquals(tenantId, tool.getTenantId());

        tool = new PhoenixTTLTool();
        tool.parseArgs(new String[] {"-v", viewName, "-p", "0"});
        assertEquals("VERY_HIGH", tool.getJobPriority());
        assertEquals(false, tool.isDeletingAllViews());
        assertEquals(viewName, tool.getViewName());
        assertEquals(null, tool.getTenantId());

        tool = new PhoenixTTLTool();
        tool.parseArgs(new String[] {"-v", viewName, "-p", "-1"});
        assertEquals("NORMAL", tool.getJobPriority());
        assertEquals(false, tool.isDeletingAllViews());
        assertEquals(viewName, tool.getViewName());
        assertEquals(null, tool.getTenantId());

        tool = new PhoenixTTLTool();
        tool.parseArgs(new String[] {"-v", viewName, "-p", "DSAFDAS"});
        assertEquals("NORMAL", tool.getJobPriority());
        assertEquals(false, tool.isDeletingAllViews());
        assertEquals(viewName, tool.getViewName());
        assertEquals(null, tool.getTenantId());

        tool = new PhoenixTTLTool();
        tool.parseArgs(new String[] {"-i", tenantId});
        assertEquals("NORMAL", tool.getJobPriority());
        assertEquals(false, tool.isDeletingAllViews());
        assertEquals(null, tool.getViewName());
        assertEquals(tenantId, tool.getTenantId());
    }

    @Test (expected = IllegalStateException.class)
    public void testNoInputParam() {
        PhoenixTTLTool tool;
        tool = new PhoenixTTLTool();
        tool.parseOptions(new String[] {});
    }
}