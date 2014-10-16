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

import org.junit.Test;

public class JDBCUtilTest {
    
    @Test
    public void testRemoveProperty() {
        assertEquals("localhost;", JDBCUtil.removeProperty("localhost;TenantId=abc;", TENANT_ID_ATTRIB));
        assertEquals("localhost;foo=bar", JDBCUtil.removeProperty("localhost;TenantId=abc;foo=bar", TENANT_ID_ATTRIB));
        assertEquals("localhost;TenantId=abc", JDBCUtil.removeProperty("localhost;TenantId=abc;foo=bar", "foo"));
        assertEquals("localhost;TenantId=abc;foo=bar", JDBCUtil.removeProperty("localhost;TenantId=abc;foo=bar", "bar"));
    }
}
