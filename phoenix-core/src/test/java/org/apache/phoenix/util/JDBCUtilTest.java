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

import static org.apache.phoenix.util.PhoenixRuntime.ANNOTATION_ATTRIB_PREFIX;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Consistency;
import org.junit.Test;

public class JDBCUtilTest {
    
    @Test
    public void testGetCustomTracingAnnotationsWithNone() {
        String url = "localhost;TenantId=abc;";
        Map<String, String> customAnnotations = JDBCUtil.getAnnotations(url, new Properties());
        assertTrue(customAnnotations.isEmpty());
    }
    
    @Test
    public void testGetCustomTracingAnnotationInBothPropertiesAndURL() {
        String annotKey1 = "key1";
        String annotVal1 = "val1";
        String annotKey2 = "key2";
        String annotVal2 = "val2";
        String annotKey3 = "key3";
        String annotVal3 = "val3";
        
        String url= "localhost;" + ANNOTATION_ATTRIB_PREFIX + annotKey1 + '=' + annotVal1;
        
        Properties prop = new Properties();
        prop.put(ANNOTATION_ATTRIB_PREFIX + annotKey2, annotVal2);
        prop.put(ANNOTATION_ATTRIB_PREFIX + annotKey3, annotVal3);
        
        Map<String, String> customAnnotations = JDBCUtil.getAnnotations(url, prop);
        assertEquals(3, customAnnotations.size());
        assertEquals(annotVal1, customAnnotations.get(annotKey1));
        assertEquals(annotVal2, customAnnotations.get(annotKey2));
        assertEquals(annotVal3, customAnnotations.get(annotKey3));
    }

    @Test
    public void testRemoveProperty() {
        assertEquals("localhost;", JDBCUtil.removeProperty("localhost;TenantId=abc;", TENANT_ID_ATTRIB));
        assertEquals("localhost;foo=bar", JDBCUtil.removeProperty("localhost;TenantId=abc;foo=bar", TENANT_ID_ATTRIB));
        assertEquals("localhost;TenantId=abc", JDBCUtil.removeProperty("localhost;TenantId=abc;foo=bar", "foo"));
        assertEquals("localhost;TenantId=abc;foo=bar", JDBCUtil.removeProperty("localhost;TenantId=abc;foo=bar", "bar"));
    }

    @Test
    public void testGetAutoCommit_NotSpecified_DefaultTrue() {
        assertTrue(JDBCUtil.getAutoCommit("localhost", new Properties(), true));
    }


    @Test
    public void testGetAutoCommit_NotSpecified_DefaultFalse() {
        assertFalse(JDBCUtil.getAutoCommit("localhost", new Properties(), false));
    }

    @Test
    public void testGetAutoCommit_TrueInUrl() {
        assertTrue(JDBCUtil.getAutoCommit("localhost;AutoCommit=TrUe", new Properties(), false));
    }

    @Test
    public void testGetAutoCommit_FalseInUrl() {
        assertFalse(JDBCUtil.getAutoCommit("localhost;AutoCommit=FaLse", new Properties(), false));
    }

    @Test
    public void testGetAutoCommit_TrueInProperties() {
        Properties props = new Properties();
        props.setProperty("AutoCommit", "true");
        assertTrue(JDBCUtil.getAutoCommit("localhost", props, false));
    }

    @Test
    public void testGetAutoCommit_FalseInProperties() {
        Properties props = new Properties();
        props.setProperty("AutoCommit", "false");
        assertFalse(JDBCUtil.getAutoCommit("localhost", props, false));
    }

    @Test
    public void testGetConsistency_TIMELINE_InUrl() {
        assertTrue(JDBCUtil.getConsistencyLevel("localhost;Consistency=TIMELINE", new Properties(),
                Consistency.STRONG.toString()) == Consistency.TIMELINE);
    }

    @Test
    public void testGetConsistency_TIMELINE_InProperties() {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CONSISTENCY_ATTRIB, "TIMELINE");
        assertTrue(JDBCUtil.getConsistencyLevel("localhost", props, Consistency.STRONG.toString())
                == Consistency.TIMELINE);
    }
}
