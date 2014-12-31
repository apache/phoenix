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


import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Test;

public class JDBCUtilTest {
    
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
}
