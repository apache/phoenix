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

import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceName;
import static org.apache.phoenix.util.MetaDataUtil.getViewIndexSequenceSchemaName;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;


public class TenantSpecificViewIndexIT extends BaseTenantSpecificViewIndexIT {
	
    @Test
    public void testUpdatableView() throws Exception {
        testUpdatableView(null);
    }

    @Test
    public void testUpdatableViewLocalIndex() throws Exception {
        testUpdatableView(null, true);
    }

    @Test
    public void testUpdatableViewLocalIndexNonStringTenantId() throws Exception {
        testUpdatableViewNonString(null, true);
    }

    @Test
    public void testUpdatableViewsWithSameNameDifferentTenants() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(null);
    }

    @Test
    public void testUpdatableViewsWithSameNameDifferentTenantsWithLocalIndex() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(null, true);
    }


    @Test
    public void testMultiCFViewIndex() throws Exception {
        testMultiCFViewIndex(false, false);
    }


    @Test
    public void testMultiCFViewIndexWithNamespaceMapping() throws Exception {
        testMultiCFViewIndex(false, true);
    }


    @Test
    public void testMultiCFViewLocalIndex() throws Exception {
        testMultiCFViewIndex(true, false);
    }

    private void createTableAndValidate(String tableName, boolean isNamespaceEnabled) throws Exception {
        Properties props = new Properties();
        if (isNamespaceEnabled) {
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        if (isNamespaceEnabled) {
            conn.createStatement().execute("CREATE SCHEMA " + SchemaUtil.getSchemaNameFromFullName(tableName));
        }
        String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR not null, PK2 VARCHAR not null, "
                + "MYCF1.COL1 varchar,MYCF2.COL2 varchar " + "CONSTRAINT pk PRIMARY KEY(PK1,PK2)) MULTI_TENANT=true";
        conn.createStatement().execute(ddl);

        conn.createStatement().execute("UPSERT INTO " + tableName + " values ('a','b','c','d')");
        conn.commit();

        ResultSet rs = conn.createStatement()
                .executeQuery("select * from " + tableName + " where (pk1,pk2) IN (('a','b'),('b','b'))");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("b", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    private void testMultiCFViewIndex(boolean localIndex, boolean isNamespaceEnabled) throws Exception {
        String tableName = "XYZ." + generateUniqueName();
        String baseViewName = generateUniqueName() ;
        createTableAndValidate(tableName, isNamespaceEnabled);
        createViewAndIndexesWithTenantId(tableName, baseViewName, localIndex, "b", isNamespaceEnabled);
        createViewAndIndexesWithTenantId(tableName, baseViewName, localIndex, "a", isNamespaceEnabled);

        String sequenceNameA = getViewIndexSequenceName(PNameFactory.newName(tableName), PNameFactory.newName("a"), isNamespaceEnabled);
        String sequenceNameB = getViewIndexSequenceName(PNameFactory.newName(tableName), PNameFactory.newName("b"), isNamespaceEnabled);
        String sequenceSchemaName = getViewIndexSequenceSchemaName(PNameFactory.newName(tableName), isNamespaceEnabled);
        verifySequenceValue(isNamespaceEnabled? "a" : null, sequenceNameA, sequenceSchemaName, -32767);
        verifySequenceValue(isNamespaceEnabled? "b" : null, sequenceNameB, sequenceSchemaName, -32767);

        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "a");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("DROP VIEW  " + baseViewName + "_a");
        }
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "b");
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("DROP VIEW  " + baseViewName + "_b");
        }
        DriverManager.getConnection(getUrl()).createStatement().execute("DROP TABLE " + tableName + " CASCADE");

        verifySequenceNotExists(isNamespaceEnabled? "a" : null, sequenceNameA, sequenceSchemaName);
        verifySequenceNotExists(isNamespaceEnabled? "b" : null, sequenceNameB, sequenceSchemaName);
    }

    private void createViewAndIndexesWithTenantId(String tableName,String baseViewName, boolean localIndex, String tenantId,
            boolean isNamespaceMapped) throws Exception {
        Properties props = new Properties();
        String viewName = baseViewName + "_" + tenantId;
        String indexName = "idx_" + viewName;
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
        ResultSet rs = conn.createStatement().executeQuery("select * from " + viewName);

        int i = 1;
        if ("a".equals(tenantId)) {
            assertTrue(rs.next());
            assertEquals("b", rs.getString(i++));
            assertEquals("c", rs.getString(i++));
            assertEquals("d", rs.getString(i++));
        }
        assertFalse(rs.next());
        conn.createStatement().execute("UPSERT INTO " + viewName + " VALUES ('e','f','g')");
        conn.commit();
        if (localIndex) {
            conn.createStatement().execute("create local index " + indexName + " on " + viewName + " (COL1)");
        } else {
            conn.createStatement().execute("create index " + indexName + " on " + viewName + " (COL1)");
        }
        rs = conn.createStatement().executeQuery("select * from " + viewName);
        i = 1;
        if ("a".equals(tenantId)) {
            assertTrue(rs.next());
            assertEquals("b", rs.getString(i++));
            assertEquals("c", rs.getString(i++));
            assertEquals("d", rs.getString(i++));
        }
        assertTrue(rs.next());
        assertEquals("e", rs.getString(1));
        assertEquals("f", rs.getString(2));
        assertEquals("g", rs.getString(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("explain select * from " + viewName);
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                + SchemaUtil.getPhysicalHBaseTableName(tableName, isNamespaceMapped, PTableType.TABLE) + " ['"
                + tenantId + "']", QueryUtil.getExplainPlan(rs));

        rs = conn.createStatement().executeQuery("select pk2,col1 from " + viewName + " where col1='f'");
        assertTrue(rs.next());
        assertEquals("e", rs.getString(1));
        assertEquals("f", rs.getString(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("explain select pk2,col1 from " + viewName + " where col1='f'");
        if (localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + SchemaUtil.getPhysicalHBaseTableName(tableName, isNamespaceMapped, PTableType.TABLE) + " [1,'"
                    + tenantId + "','f']\n" + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        } else {
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER "
                    + Bytes.toString(MetaDataUtil.getViewIndexPhysicalName(SchemaUtil
                            .getPhysicalHBaseTableName(tableName, isNamespaceMapped, PTableType.TABLE).getBytes()))
                    + " [-32768,'" + tenantId + "','f']\n" + "    SERVER FILTER BY FIRST KEY ONLY",
                    QueryUtil.getExplainPlan(rs));
        }

        try {
            // Cannot reference tenant_id column in tenant specific connection
            conn.createStatement()
                    .executeQuery("select * from " + tableName + " where (pk1,pk2) IN (('a','b'),('b','b'))");
            if (tenantId != null) {
                fail();
            }
        } catch (ColumnNotFoundException e) {
            if (tenantId == null) {
                fail();
            }
        }

        // This is ok, though
        rs = conn.createStatement().executeQuery("select * from " + tableName + " where pk2 IN ('b','e')");
        if ("a".equals(tenantId)) {
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
        }
        assertTrue(rs.next());
        assertEquals("e", rs.getString(1));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("select * from " + viewName + " where pk2 IN ('b','e')");
        if ("a".equals(tenantId)) {
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
        }
        assertTrue(rs.next());
        assertEquals("e", rs.getString(1));
        assertFalse(rs.next());

        conn.close();

    }
    
    @Test
    public void testNonPaddedTenantId() throws Exception {
        String tenantId1 = "org1";
        String tenantId2 = "org2";
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (tenantId char(15) NOT NULL, pk1 varchar NOT NULL, pk2 INTEGER NOT NULL, val1 VARCHAR CONSTRAINT pk primary key (tenantId,pk1,pk2)) MULTI_TENANT = true";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO " + tableName + " (tenantId, pk1, pk2, val1) VALUES (?, ?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        
        String pk = "pk1b";
        // insert two rows in table T. One for tenantId1 and other for tenantId2.
        stmt.setString(1, tenantId1);
        stmt.setString(2, pk);
        stmt.setInt(3, 100);
        stmt.setString(4, "value1");
        stmt.executeUpdate();
        
        stmt.setString(1, tenantId2);
        stmt.setString(2, pk);
        stmt.setInt(3, 200);
        stmt.setString(4, "value2");
        stmt.executeUpdate();
        conn.commit();
        conn.close();
        
        // get a tenant specific url.
        String tenantUrl = getUrl() + ';' + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId1;
        Connection tenantConn = DriverManager.getConnection(tenantUrl);
        
        // create a tenant specific view.
        tenantConn.createStatement().execute("CREATE VIEW " + viewName + " AS select * from " + tableName);
        String query = "SELECT val1 FROM " + viewName + " WHERE pk1 = ?";
        
        // using the tenant connection query the view.
        PreparedStatement stmt2 = tenantConn.prepareStatement(query);
        stmt2.setString(1, pk); // for tenantId1 the row inserted has pk1 = "pk1b"
        ResultSet rs = stmt2.executeQuery();
        assertTrue(rs.next());
        assertEquals("value1", rs.getString(1));
        assertFalse("No other rows should have been returned for the tenant", rs.next()); // should have just returned one record since for org1 we have only one row.
    }
    
    @Test
    public void testOverlappingDatesFilter() throws SQLException {
        String tenantUrl = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant1" + ";" + QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB + "=true";
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName 
                + "(ORGANIZATION_ID CHAR(15) NOT NULL, "
                + "PARENT_TYPE CHAR(3) NOT NULL, "
                + "PARENT_ID CHAR(15) NOT NULL,"
                + "CREATED_DATE DATE NOT NULL "
                + "CONSTRAINT PK PRIMARY KEY (ORGANIZATION_ID, PARENT_TYPE, PARENT_ID, CREATED_DATE DESC)"
                + ") VERSIONS=1,MULTI_TENANT=true,REPLICATION_SCOPE=1"; 
                
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = DriverManager.getConnection(tenantUrl) ) {
            // create table
            conn.createStatement().execute(ddl);
            // create index
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS IDX ON " + tableName + "(PARENT_TYPE, CREATED_DATE, PARENT_ID)");
            // create view
            viewConn.createStatement().execute("CREATE VIEW IF NOT EXISTS " + viewName + " AS SELECT * FROM "+ tableName );
            
            String query ="EXPLAIN SELECT PARENT_ID FROM " + viewName
                    + " WHERE PARENT_TYPE='001' "
                    + "AND (CREATED_DATE > to_date('2011-01-01') AND CREATED_DATE < to_date('2016-10-31'))"
                    + "ORDER BY PARENT_TYPE,CREATED_DATE LIMIT 501";
            
            ResultSet rs = viewConn.createStatement().executeQuery(query);
            String expectedPlanFormat = "CLIENT SERIAL 1-WAY RANGE SCAN OVER IDX ['tenant1        ','001','%s 00:00:00.001'] - ['tenant1        ','001','%s 00:00:00.000']" + "\n" +
                        "    SERVER FILTER BY FIRST KEY ONLY" + "\n" +
                        "    SERVER 501 ROW LIMIT" + "\n" +
                        "CLIENT 501 ROW LIMIT";
            assertEquals(String.format(expectedPlanFormat, "2011-01-01", "2016-10-31"), QueryUtil.getExplainPlan(rs));
            
            query ="EXPLAIN SELECT PARENT_ID FROM " + viewName
                    + " WHERE PARENT_TYPE='001' "
                    + " AND (CREATED_DATE >= to_date('2011-01-01') AND CREATED_DATE <= to_date('2016-01-01'))"
                    + " AND (CREATED_DATE > to_date('2012-10-21') AND CREATED_DATE < to_date('2016-10-31')) "
                    + "ORDER BY PARENT_TYPE,CREATED_DATE LIMIT 501";
            
            rs = viewConn.createStatement().executeQuery(query);
            assertEquals(String.format(expectedPlanFormat, "2012-10-21", "2016-01-01"), QueryUtil.getExplainPlan(rs));
        }
    }
}
