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
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PHOENIX_META_DATA_COPROCESSOR_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ViewTypeIT extends SplitSystemCatalogIT {

    private Connection conn;
    private String fullTableName;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 6;
        boolean splitSystemCatalog = (driver == null);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");
        serverProps.put(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY,
                ViewConcurrencyAndFailureIT.TestMetaDataRegionObserver.class
                        .getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                ReadOnlyProps.EMPTY_PROPS);
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }
    }

    @Before
    public void setup() throws Exception {
        conn = DriverManager.getConnection(getUrl());
        initTables(conn);
    }

    private void initTables(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        fullTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String tableDDL = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 DECIMAL, k3 INTEGER NOT NULL,"
                + " s VARCHAR "
                + "CONSTRAINT pk PRIMARY KEY (k1, k2, k3))";
        stmt.execute(tableDDL);
    }

    @Test
    public void testReadOnlyViewWithNonPkInWhere() throws Exception {
        Statement stmt = conn.createStatement();
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                " WHERE s = 'a'";
        stmt.execute(viewDDL);
        try {
            stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 3, 'a')");
            fail();
        } catch (ReadOnlyTableException ignored) {
        }
    }

    @Test
    public void testReadOnlyViewWithPkNotInOrderInWhere() throws Exception {
        Statement stmt = conn.createStatement();
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName +
                " WHERE k1 = 1 AND k3 = 3";
        stmt.execute(viewDDL);
        try {
            stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 3, 'a')");
            fail();
        } catch (ReadOnlyTableException ignored) {
        }
    }

    @Test
    public void testReadOnlyViewWithPkNotSameInWhere() throws Exception {
        Statement stmt = conn.createStatement();
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String fullSiblingViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

        String globalViewDDL = "CREATE VIEW " + fullGlobalViewName + " AS SELECT * FROM " + fullTableName +
                " WHERE k1 = 1";
        stmt.execute(globalViewDDL);

        String siblingViewDDL = "CREATE VIEW " + fullSiblingViewName + " AS SELECT * FROM " +
                fullGlobalViewName + " WHERE k2 = 1";
        stmt.execute(siblingViewDDL);
        String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullGlobalViewName
                + " WHERE k2 = 2 AND k3 = 109";
        stmt.execute(viewDDL);
        try {
            stmt.execute("UPSERT INTO " + fullViewName + " VALUES(1, 2, 109, 'a')");
            fail();
        } catch (ReadOnlyTableException ignored) {
        }
    }

    @Test
    public void testUpdatableViewOnView() throws Exception {
        Statement stmt = conn.createStatement();
        String fullGlobalViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        String fullLeafViewName1 = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());
        String fullLeafViewName2 = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());

        String globalViewDDL = "CREATE VIEW " + fullGlobalViewName + " AS SELECT * FROM "
                + fullTableName + " WHERE k1 = 1";
        stmt.execute(globalViewDDL);
        String viewDDL = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullGlobalViewName
                + " WHERE k2 = 1";
        stmt.execute(viewDDL);
        String leafView1DDL = "CREATE VIEW " + fullLeafViewName1 + " AS SELECT * FROM "
                + fullViewName + " WHERE k3 = 101";
        stmt.execute(leafView1DDL);
        String leafView2DDL = "CREATE VIEW " + fullLeafViewName2 + " AS SELECT * FROM "
                + fullViewName + " WHERE k3 = 105";
        stmt.execute(leafView2DDL);

        for (int i = 0; i < 10; i++) {
            stmt.execute("UPSERT INTO " + fullTableName
                    + " VALUES(" + (i % 4) + "," + (i > 5 ? 2 : 1) + "," + (i + 100) + ")");
        }
        conn.commit();

        ResultSet rs;
        rs = stmt.executeQuery("SELECT count(*) FROM " + fullTableName);
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        rs = stmt.executeQuery("SELECT count(*) FROM " + fullGlobalViewName);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs = stmt.executeQuery("SELECT count(*) FROM " + fullViewName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullLeafViewName1);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(101, rs.getInt(3));
        assertFalse(rs.next());
        rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullLeafViewName2);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(105, rs.getInt(3));
        assertFalse(rs.next());
    }
}
