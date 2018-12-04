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
package org.apache.phoenix.end2end.salted;

import static org.apache.phoenix.util.TestUtil.TABLE_WITH_SALTING;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

/**
 * Tests for table with transparent salting.
 */

public class SaltedTableIT extends BaseSaltedTableIT {

    @Test
    public void testTableWithInvalidBucketNumber() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "create table " + generateUniqueName() + " (a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS = 257";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            fail("Should have caught exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1021 (42Y80): Salt bucket numbers should be with 1 and 256."));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableWithSplit() throws Exception {
        try {
            createTestTable(getUrl(), "create table " + generateUniqueName() + " (a_integer integer not null primary key) SALT_BUCKETS = 4",
                    new byte[][] {{1}, {2,3}, {2,5}, {3}}, null);
            fail("Should have caught exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1022 (42Y81): Should not specify split points on salted table with default row key order."));
        }
    }

}
