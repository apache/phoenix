/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class LogUtilTest {
    
	@Mock PhoenixConnection con;
	
    @Test
    public void testAddCustomAnnotationsWithNullConnection() {
    	String logLine = LogUtil.addCustomAnnotations("log line", (PhoenixConnection)null);
    	assertEquals(logLine, "log line");
    }
	
    @Test
    public void testAddCustomAnnotationsWithNullAnnotations() {
    	when(con.getCustomTracingAnnotations()).thenReturn(null);
    	
    	String logLine = LogUtil.addCustomAnnotations("log line", con);
    	assertEquals(logLine, "log line");
    }
    
    @Test
    public void testAddCustomAnnotations() {
    	when(con.getCustomTracingAnnotations()).thenReturn(ImmutableMap.of("a1", "v1", "a2", "v2"));
    	
    	String logLine = LogUtil.addCustomAnnotations("log line", con);
    	assertTrue(logLine.contains("log line"));
    	assertTrue(logLine.contains("a1=v1"));
    	assertTrue(logLine.contains("a2=v2"));
    }
}
