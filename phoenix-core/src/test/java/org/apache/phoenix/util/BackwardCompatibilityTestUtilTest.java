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
package org.apache.phoenix.util;

import org.junit.Test;

import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.getHBaseProfile;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.getPhoenixClientVersion;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.isUsingNewNamingScheme;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackwardCompatibilityTestUtilTest {
    private String OLD_PHOENIX_VERSION_STRING = "4.15.0";
    private String OLD_HBASE_PROFILE_STRING = "1.3";
    private String NEW_PHOENIX_VERSION_STRING = "4.16.0";
    private String NEW_HBASE_PROFILE_STRING = "1.6";
    private String OLD_PHOENIX_CLIENT_SCHEME_STRING = "4.15.0-HBase-1.3";
    private String NEW_PHOENIX_CLIENT_SCHEM_STRING = "4.16.0-HBase-1.6";

    @Test
    public void testNamingScheme() {
        assertTrue(isUsingNewNamingScheme(NEW_PHOENIX_CLIENT_SCHEM_STRING));
        assertFalse(isUsingNewNamingScheme(OLD_PHOENIX_CLIENT_SCHEME_STRING));
    }

    @Test
    public void testGetHBaseProfile() {
        assertEquals(NEW_HBASE_PROFILE_STRING, getHBaseProfile(NEW_PHOENIX_CLIENT_SCHEM_STRING));
        assertEquals(OLD_HBASE_PROFILE_STRING, getHBaseProfile(OLD_PHOENIX_CLIENT_SCHEME_STRING));
    }

    @Test
    public void testGetPhoenixClientVersion() {
        assertEquals(NEW_PHOENIX_VERSION_STRING,
                getPhoenixClientVersion(NEW_PHOENIX_CLIENT_SCHEM_STRING));
        assertEquals(OLD_PHOENIX_VERSION_STRING,
                getPhoenixClientVersion(OLD_PHOENIX_CLIENT_SCHEME_STRING));
    }
}
