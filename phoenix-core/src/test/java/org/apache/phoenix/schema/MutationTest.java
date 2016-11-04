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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class MutationTest extends BaseConnectionlessQueryTest {
    @Test
    public void testDurability() throws Exception {
        testDurability(true);
        testDurability(false);
    }

    private void testDurability(boolean disableWAL) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            Durability expectedDurability = disableWAL ? Durability.SKIP_WAL : Durability.USE_DEFAULT;
            conn.setAutoCommit(false);
            conn.createStatement().execute("CREATE TABLE t1 (k integer not null primary key, a.k varchar, b.k varchar) " + (disableWAL ? "DISABLE_WAL=true" : ""));
            conn.createStatement().execute("UPSERT INTO t1 VALUES(1,'a','b')");
            conn.createStatement().execute("DELETE FROM t1 WHERE k=2");
            assertDurability(conn,expectedDurability);
            conn.createStatement().execute("DELETE FROM t1 WHERE k=1");
            assertDurability(conn,expectedDurability);
            conn.createStatement().execute("DROP TABLE t1");
        } finally {
            conn.close();
        }
    }
    
    private void assertDurability(Connection conn, Durability durability) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        Iterator<Pair<byte[], List<Mutation>>> it = pconn.getMutationState().toMutations();
        assertTrue(it.hasNext());
        while (it.hasNext()) {
            Pair<byte[], List<Mutation>> pair = it.next();
            assertFalse(pair.getSecond().isEmpty());
            for (Mutation m : pair.getSecond()) {
                assertEquals(durability, m.getDurability());
            }
        }
    }
    
    @Test
    public void testSizeConstraint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            int maxLength1 = 3;
            int maxLength2 = 20;
            conn.setAutoCommit(false);
            String bvalue = "01234567890123456789";
            assertEquals(20,PVarchar.INSTANCE.toBytes(bvalue).length);
            String value = "澴粖蟤य褻酃岤豦팑薰鄩脼ժ끦碉碉碉碉碉";
            assertTrue(value.length() <= maxLength2);
            assertTrue(PVarchar.INSTANCE.toBytes(value).length > maxLength2);
            conn.createStatement().execute("CREATE TABLE t1 (k1 char(" + maxLength1 + ") not null, k2 varchar(" + maxLength2 + "), "
                    + "v1 varchar(" + maxLength2 + "), v2 varbinary(" + maxLength2 + "), v3 binary(" + maxLength2 + "), constraint pk primary key (k1, k2))");
            conn.createStatement().execute("UPSERT INTO t1 VALUES('a','" + value + "', '" + value + "','" + bvalue + "','" + bvalue + "')");
            try {
                conn.createStatement().execute("UPSERT INTO t1(k1,v1) VALUES('abcd','" + value + "')");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            try {
                conn.createStatement().execute("UPSERT INTO t1(k1,v2) VALUES('b','" + value + "')");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            try {
                conn.createStatement().execute("UPSERT INTO t1(k1,v3) VALUES('b','" + value + "')");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            value = "澴粖蟤य褻酃岤豦팑薰鄩脼ժ끦碉碉碉碉碉碉碉碉碉";
            assertTrue(value.length() > maxLength2);
            try {
                conn.createStatement().execute("UPSERT INTO t1(k1,k2) VALUES('a','" + value + "')");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            try {
                conn.createStatement().execute("UPSERT INTO t1(k1,v1) VALUES('a','" + value + "')");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

}
