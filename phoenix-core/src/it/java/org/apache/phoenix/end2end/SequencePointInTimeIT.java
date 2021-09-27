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

package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;


@Category(NeedsOwnMiniClusterTest.class)
public class SequencePointInTimeIT extends BaseTest {
    private static final String SCHEMA_NAME = "S";

    private static String generateSequenceNameWithSchema() {
        return SchemaUtil.getTableName(SCHEMA_NAME, generateUniqueSequenceName());
    }
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        // Must update config before starting server
        props.put(QueryServices.DEFAULT_SYSTEM_KEEP_DELETED_CELLS_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.DEFAULT_SYSTEM_MAX_VERSIONS_ATTRIB, "5");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testPointInTimeSequence() throws Exception {
        String seqName = generateSequenceNameWithSchema();
        Properties scnProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        scnProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(EnvironmentEdgeManager.currentTimeMillis()));
        Connection beforeSeqConn = DriverManager.getConnection(getUrl(), scnProps);

        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE SEQUENCE " + seqName + "");

        try {
            beforeSeqConn.createStatement().executeQuery("SELECT next value for " + seqName);
            fail();
        } catch (SequenceNotFoundException e) {
            beforeSeqConn.close();
        }

        scnProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(EnvironmentEdgeManager.currentTimeMillis()));
        Connection afterSeqConn = DriverManager.getConnection(getUrl(), scnProps);

        rs = conn.createStatement().executeQuery("SELECT next value for " + seqName);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT next value for " + seqName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        conn.createStatement().execute("DROP SEQUENCE " + seqName + "");

        rs = afterSeqConn.createStatement().executeQuery("SELECT next value for " + seqName);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        try {
            rs = conn.createStatement().executeQuery("SELECT next value for " + seqName);
            fail();
        } catch (SequenceNotFoundException e) { // expected
        }

        conn.createStatement().execute("CREATE SEQUENCE " + seqName);
        rs = conn.createStatement().executeQuery("SELECT next value for " + seqName);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        rs = afterSeqConn.createStatement().executeQuery("SELECT next value for " + seqName);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        afterSeqConn.close();
    }

}