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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Properties;

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
}
