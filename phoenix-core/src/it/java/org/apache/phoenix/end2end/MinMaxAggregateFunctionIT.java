/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class MinMaxAggregateFunctionIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testMinMaxAggregateFunctions() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            conn.prepareStatement(
                "create table TT("
                        + "VAL1 integer not null, "
                        + "VAL2 char(2), "
                        + "VAL3 varchar, "
                        + "VAL4 varchar "
                        + "constraint PK primary key (VAL1))").execute();
            conn.commit();

            conn.prepareStatement("upsert into TT values (0, '00', '00', '0')").execute();
            conn.prepareStatement("upsert into TT values (1, '01', '01', '1')").execute();
            conn.prepareStatement("upsert into TT values (2, '02', '02', '2')").execute();
            conn.commit();

            ResultSet rs = conn.prepareStatement("select min(VAL2) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("00", rs.getString(1));

            rs = conn.prepareStatement("select min(VAL3) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("00", rs.getString(1));

            rs = conn.prepareStatement("select max(VAL2)from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("02", rs.getString(1));

            rs = conn.prepareStatement("select max(VAL3)from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals("02", rs.getString(1));

            rs =
                    conn.prepareStatement(
                        "select min(VAL1), min(VAL2), min(VAL3), min(VAL4) from TT").executeQuery();
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertEquals("00", rs.getString(2));
            assertEquals("00", rs.getString(3));
            assertEquals("0", rs.getString(4));

            rs =
                    conn.prepareStatement(
                        "select max(VAL1), max(VAL2), max(VAL3), max(VAL4) from TT").executeQuery();

            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("02", rs.getString(2));
            assertEquals("02", rs.getString(3));
            assertEquals("2", rs.getString(4));
        } finally {
            conn.close();
        }
    }
}
