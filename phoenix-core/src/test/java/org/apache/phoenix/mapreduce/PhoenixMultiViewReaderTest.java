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

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.mapreduce.util.ViewInfoTracker;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class PhoenixMultiViewReaderTest {

    @Test
    public void test() throws Exception {
        String tenantId = "Tenant1";
        String viewName = "viewName1";
        long ttl = 1;
        String indexTable = "indexTable";
        String globalView = "globalView";

        PhoenixMultiViewInputSplit mockInput = Mockito.mock(PhoenixMultiViewInputSplit.class);
        TaskAttemptContext mockContext = Mockito.mock(TaskAttemptContext.class);
        List<ViewInfoWritable> viewInfoTracker = new ArrayList<>();
        viewInfoTracker.add(new ViewInfoTracker(
                tenantId,
                viewName,
                ttl,
                globalView,
                false
        ));

        viewInfoTracker.add(new ViewInfoTracker(
                tenantId,
                viewName,
                ttl,
                indexTable,
                true
        ));
        when(mockInput.getViewInfoTrackerList()).thenReturn(viewInfoTracker);
        PhoenixMultiViewReader phoenixMultiViewReader = new PhoenixMultiViewReader();
        phoenixMultiViewReader.initialize(mockInput, mockContext);

        ViewInfoTracker viewInfoWritable;
        assertTrue(phoenixMultiViewReader.nextKeyValue());
        viewInfoWritable = (ViewInfoTracker)phoenixMultiViewReader.getCurrentValue();
        assertEquals(tenantId, viewInfoWritable.getTenantId());
        assertEquals(viewName, viewInfoWritable.getViewName());
        assertEquals(ttl, viewInfoWritable.getPhoenixTtl());
        assertEquals(false, viewInfoWritable.isIndexRelation());

        assertTrue(phoenixMultiViewReader.nextKeyValue());
        viewInfoWritable = (ViewInfoTracker)phoenixMultiViewReader.getCurrentValue();
        assertEquals(tenantId, viewInfoWritable.getTenantId());
        assertEquals(viewName, viewInfoWritable.getViewName());
        assertEquals(ttl, viewInfoWritable.getPhoenixTtl());
        assertEquals(true, viewInfoWritable.isIndexRelation());

        assertFalse(phoenixMultiViewReader.nextKeyValue());
        viewInfoWritable = (ViewInfoTracker)phoenixMultiViewReader.getCurrentValue();
        assertNull(phoenixMultiViewReader.getCurrentValue());
    }
}