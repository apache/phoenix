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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ReadOnlyPropertiesTest {
    
    private static final String PROPERTY_NAME_1 = "property1";
    private static final String DEFAULT_VALUE_1 = "default1";
    private static final String OVERRIDEN_VALUE_1 = "override1";
    private static final String OVERRIDEN_VALUE_2 = "override2";
    
    private static final String PROPERTY_NAME_2 = "property2";
    private static final String DEFAULT_VALUE_2 = "default2";
    
    private static final Map<String, String> EMPTY_OVERRIDE_MAP = Collections.emptyMap();
    private static final Map<String, String> DEFAULT_PROPS_MAP = ImmutableMap.of(PROPERTY_NAME_1, DEFAULT_VALUE_1, PROPERTY_NAME_2, DEFAULT_VALUE_2);
    private static final Map<String, String> OVERRIDE_MAP = ImmutableMap.of(PROPERTY_NAME_1, OVERRIDEN_VALUE_1);
    
    @Test
    public void testDefaultProperties() {
        ReadOnlyProps defaultProps = new ReadOnlyProps(DEFAULT_PROPS_MAP);
        ReadOnlyProps readOnlyProps = new ReadOnlyProps(defaultProps, EMPTY_OVERRIDE_MAP.entrySet().iterator());
        assertEquals(DEFAULT_VALUE_1, readOnlyProps.get(PROPERTY_NAME_1));
        assertEquals(DEFAULT_VALUE_2, readOnlyProps.get(PROPERTY_NAME_2));
    }
    
    @Test
    public void testOverrideProperties() {
        ReadOnlyProps defaultProps = new ReadOnlyProps(DEFAULT_PROPS_MAP);
        ReadOnlyProps readOnlyProps = new ReadOnlyProps(defaultProps, OVERRIDE_MAP.entrySet().iterator());
        assertEquals(OVERRIDEN_VALUE_1, readOnlyProps.get(PROPERTY_NAME_1));
        assertEquals(DEFAULT_VALUE_2, readOnlyProps.get(PROPERTY_NAME_2));
    }
    
    @Test
    public void testAddAllOverrideProperties() {
        ReadOnlyProps defaultProps = new ReadOnlyProps(DEFAULT_PROPS_MAP);
        Properties overrideProps = new Properties();
        overrideProps.setProperty(PROPERTY_NAME_1, OVERRIDEN_VALUE_1);
        ReadOnlyProps newProps = defaultProps.addAll(overrideProps);
        assertEquals(OVERRIDEN_VALUE_1, newProps.get(PROPERTY_NAME_1));
        assertEquals(DEFAULT_VALUE_2, newProps.get(PROPERTY_NAME_2));
    }
    
    @Test
    public void testOverridingNonDefaultProperties() {
        ReadOnlyProps defaultProps = new ReadOnlyProps(DEFAULT_PROPS_MAP);
        Properties props = new Properties();
        props.setProperty(PROPERTY_NAME_1, OVERRIDEN_VALUE_1);
        ReadOnlyProps nonDefaultProps = defaultProps.addAll(props);
        
        Properties overrideProps = new Properties();
        overrideProps.setProperty(PROPERTY_NAME_1, OVERRIDEN_VALUE_2);
        ReadOnlyProps newProps = nonDefaultProps.addAll(overrideProps);
        assertEquals(OVERRIDEN_VALUE_2, newProps.get(PROPERTY_NAME_1));
        assertEquals(DEFAULT_VALUE_2, newProps.get(PROPERTY_NAME_2));
    }
}
