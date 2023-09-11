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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.Repeat;
import org.apache.phoenix.util.RunUntilFailure;
import org.apache.phoenix.util.TestUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(ParallelStatsDisabledTest.class)
@RunWith(RunUntilFailure.class)
public class ConcurrentMutationsIT extends ParallelStatsDisabledIT {


    private static class MyClock extends EnvironmentEdge {
        public volatile long time;
        boolean shouldAdvance = true;

        public MyClock (long time) {
            this.time = time;
        }

        @Override
        public long currentTime() {
            if (shouldAdvance) {
                return time++;
            } else {
                return time;
            }
        }
        public void setAdvance(boolean val) {
            shouldAdvance = val;
        }
    }

    @Test
    @Ignore("PHOENIX-4058 Generate correct index updates when DeleteColumn processed before Put with same timestamp")
    public void testSetIndexedColumnToNullAndValueAtSameTS() throws Exception {
        try {
            final MyClock clock = new MyClock(1000);
            EnvironmentEdgeManager.injectEdge(clock);
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0");
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
            stmt.setTimestamp(1, new Timestamp(1000L));
            stmt.executeUpdate();
            conn.commit();
            conn.close();

            Timestamp expectedTimestamp;
            clock.setAdvance(false);
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
            expectedTimestamp = null;
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            conn.commit();
            stmt.setTimestamp(1, new Timestamp(3000L));
            stmt.executeUpdate();
            conn.commit();
            clock.setAdvance(true);
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);

            IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        

            ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(expectedTimestamp, rs.getTimestamp(1));
            assertEquals(null, rs.getString(2));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
            assertTrue(rs.next());
            assertEquals(expectedTimestamp, rs.getTimestamp(1));
            assertEquals(null, rs.getString(2));
            assertFalse(rs.next());

            conn.close();
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    public void testSetIndexedColumnToNullAndValueAtSameTSWithStoreNulls1() throws Exception {
        try {
            final MyClock clock = new MyClock(1000);
            EnvironmentEdgeManager.injectEdge(clock);
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
            stmt.setTimestamp(1, new Timestamp(1000L));
            stmt.executeUpdate();
            conn.commit();
            conn.close();

            Timestamp expectedTimestamp;
            clock.setAdvance(false);
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
            expectedTimestamp = null;
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            conn.commit();
            expectedTimestamp = new Timestamp(3000L);
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            clock.setAdvance(true);
            conn.commit();
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);

            IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        

            ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(expectedTimestamp, rs.getTimestamp(1));
            assertEquals(null, rs.getString(2));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
            assertTrue(rs.next());
            assertEquals(expectedTimestamp, rs.getTimestamp(1));
            assertEquals(null, rs.getString(2));
            assertFalse(rs.next());

            conn.close();
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    public void testSetIndexedColumnToNullAndValueAtSameTSWithStoreNulls2() throws Exception {
        try {
            final MyClock clock = new MyClock(1000);
            EnvironmentEdgeManager.injectEdge(clock);
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            long ts = 1000;
            clock.time = ts;
            Connection conn = DriverManager.getConnection(getUrl(), props);     
            conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.close();

            ts = 1010;
            clock.time = ts;
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
            conn.close();

            ts = 1020;
            clock.time = ts;
            conn = DriverManager.getConnection(getUrl(), props);        
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
            stmt.setTimestamp(1, new Timestamp(1000L));
            stmt.executeUpdate();
            conn.commit();
            conn.close();

            Timestamp expectedTimestamp;
            ts = 1040;
            clock.time = ts;
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
            expectedTimestamp = new Timestamp(3000L);
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            conn.commit();
            expectedTimestamp = null;
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            conn.commit();
            conn.close();

            ts = 1050;
            clock.time = ts;
            conn = DriverManager.getConnection(getUrl(), props);

            IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);        

            ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ ts,v FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(expectedTimestamp, rs.getTimestamp(1));
            assertEquals(null, rs.getString(2));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT \"0:TS\", \"0:V\" FROM " + indexName);
            assertTrue(rs.next());
            assertEquals(expectedTimestamp, rs.getTimestamp(1));
            assertEquals(null, rs.getString(2));
            assertFalse(rs.next());

            conn.close();
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
    @Ignore ("It is not possible to assign the same timestamp two separately committed mutations in the current model\n" +
            " except when the server time goes backward. In that case, the behavior is not deterministic")
    @Test
    public void testDeleteRowAndUpsertValueAtSameTS1() throws Exception {
        try {
            final MyClock clock = new MyClock(1000);
            EnvironmentEdgeManager.injectEdge(clock);
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            long ts = 1000;
            clock.time = ts;
            Connection conn = DriverManager.getConnection(getUrl(), props);     
            conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, A.V VARCHAR, B.V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.close();
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
            conn.close();
            conn = DriverManager.getConnection(getUrl(), props);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0','1')");
            stmt.setTimestamp(1, new Timestamp(1000L));
            stmt.executeUpdate();
            conn.commit();
            conn.close();

            Timestamp expectedTimestamp;
            clock.setAdvance(false);
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE (K1,K2) = ('aa','aa')");
            stmt.executeUpdate();
            conn.commit();
            expectedTimestamp = new Timestamp(6000L);
            stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null,'3')");
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            conn.commit();
            clock.setAdvance(true);
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);

            long rowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
            assertEquals(0,rowCount);

            conn.close();
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }

    @Test
    public void testDeleteRowAndUpsertValueAtSameTS2() throws Exception {
        try {
            final MyClock clock = new MyClock(1000);
            EnvironmentEdgeManager.injectEdge(clock);
            String tableName = generateUniqueName();
            String indexName = generateUniqueName();
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE TABLE " + tableName + "(k1 CHAR(2) NOT NULL, k2 CHAR(2) NOT NULL, ts TIMESTAMP, V VARCHAR, V2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES = 0, STORE_NULLS=true");
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + "(k2,k1,ts) INCLUDE (V, v2)");
            conn.close();

            conn = DriverManager.getConnection(getUrl(), props);
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, '0')");
            stmt.setTimestamp(1, new Timestamp(1000L));
            stmt.executeUpdate();
            conn.commit();
            conn.close();

            Timestamp expectedTimestamp;
            clock.setAdvance(false);
            conn = DriverManager.getConnection(getUrl(), props);
            expectedTimestamp = new Timestamp(3000L);
            stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('aa','aa',?, null)");
            stmt.setTimestamp(1, expectedTimestamp);
            stmt.executeUpdate();
            conn.commit();
            stmt = conn.prepareStatement("DELETE FROM " + tableName + " WHERE (K1,K2) = ('aa','aa')");
            stmt.executeUpdate();
            conn.commit();
            clock.setAdvance(true);
            conn.close();
            conn = DriverManager.getConnection(getUrl(), props);

            long rowCount = IndexScrutiny.scrutinizeIndex(conn, tableName, indexName);
            assertEquals(0,rowCount);

            conn.close();
        } finally {
            EnvironmentEdgeManager.injectEdge(null);
        }
    }
}
