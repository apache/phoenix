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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import com.google.common.collect.Sets;

public class PhoenixRuntimeIT extends ParallelStatsDisabledIT {
    private static void assertTenantIds(Expression e, HTableInterface htable, Filter filter, String[] tenantIds) throws IOException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner = htable.getScanner(scan);
        Result result = null;
        ResultTuple tuple;
        Set<String> actualTenantIds = Sets.newHashSetWithExpectedSize(tenantIds.length);
        Set<String> expectedTenantIds = new HashSet<>(Arrays.asList(tenantIds));
        while ((result = scanner.next()) != null) {
            tuple = new ResultTuple(result);
            e.evaluate(tuple, ptr);
            String tenantId = (String)PVarchar.INSTANCE.toObject(ptr);
            actualTenantIds.add(tenantId == null ? "" : tenantId);
        }
        assertTrue(actualTenantIds.containsAll(expectedTenantIds));
    }
    
    @Test
    public void testGetTenantIdExpressionForSaltedTable() throws Exception {
        testGetTenantIdExpression(true);
    }
    
    @Test
    public void testGetTenantIdExpressionForUnsaltedTable() throws Exception {
        testGetTenantIdExpression(false);
    }
    
    private static Filter getUserTableAndViewsFilter() {
        SingleColumnValueFilter tableFilter = new SingleColumnValueFilter(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.TABLE_TYPE_BYTES, CompareOp.EQUAL, Bytes.toBytes(PTableType.TABLE.getSerializedValue()));
        tableFilter.setFilterIfMissing(true);
        SingleColumnValueFilter viewFilter = new SingleColumnValueFilter(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.TABLE_TYPE_BYTES, CompareOp.EQUAL, Bytes.toBytes(PTableType.VIEW.getSerializedValue()));
        viewFilter.setFilterIfMissing(true);
        FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE, Arrays.asList(new Filter[] {tableFilter, viewFilter}));
        return filter;
    }
    
    private void testGetTenantIdExpression(boolean isSalted) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        String tableName = generateUniqueName() ;
        String sequenceName = generateUniqueName();
        String t1 = generateUniqueName();
        String t2 = t1 + generateUniqueName(); // ensure bigger
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2)) MULTI_TENANT=true" + (isSalted ? ",SALT_BUCKETS=3" : ""));
        conn.createStatement().execute("CREATE SEQUENCE "  + sequenceName);
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('" + t1 + "','x')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('" + t2 + "','y')");
        
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, t1);
        Connection tsconn = DriverManager.getConnection(getUrl(), props);
        tsconn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        Expression e1 = PhoenixRuntime.getTenantIdExpression(tsconn, PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME);
        HTableInterface htable1 = tsconn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES);
        assertTenantIds(e1, htable1, new FirstKeyOnlyFilter(), new String[] {"", t1} );

        String viewName = generateUniqueName();
        tsconn.createStatement().execute("CREATE VIEW " + viewName + "(V1 VARCHAR) AS SELECT * FROM " + tableName);
        Expression e2 = PhoenixRuntime.getTenantIdExpression(tsconn, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
        HTableInterface htable2 = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        assertTenantIds(e2, htable2, getUserTableAndViewsFilter(), new String[] {"", t1} );
        
        Expression e3 = PhoenixRuntime.getTenantIdExpression(conn, tableName);
        HTableInterface htable3 = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
        assertTenantIds(e3, htable3, new FirstKeyOnlyFilter(), new String[] {t1, t2} );

        String basTableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + basTableName + " (k1 VARCHAR PRIMARY KEY)");
        Expression e4 = PhoenixRuntime.getTenantIdExpression(conn, basTableName);
        assertNull(e4);

        String indexName1 = generateUniqueName();
        tsconn.createStatement().execute("CREATE INDEX " + indexName1 + " ON " + viewName + "(V1)");
        Expression e5 = PhoenixRuntime.getTenantIdExpression(tsconn, indexName1);
        HTableInterface htable5 = tsconn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(MetaDataUtil.VIEW_INDEX_TABLE_PREFIX + tableName));
        assertTenantIds(e5, htable5, new FirstKeyOnlyFilter(), new String[] {t1} );

        String indexName2 = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName2 + " ON " + tableName + "(k2)");
        Expression e6 = PhoenixRuntime.getTenantIdExpression(conn, indexName2);
        HTableInterface htable6 = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(indexName2));
        assertTenantIds(e6, htable6, new FirstKeyOnlyFilter(), new String[] {t1, t2} );
        
        tableName = generateUniqueName() + "BAR_" + (isSalted ? "SALTED" : "UNSALTED");
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2)) " + (isSalted ? "SALT_BUCKETS=3" : ""));
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('" + t1 + "','x')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('" + t2 + "','y')");
        Expression e7 = PhoenixRuntime.getFirstPKColumnExpression(conn, tableName);
        HTableInterface htable7 = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
        assertTenantIds(e7, htable7, new FirstKeyOnlyFilter(), new String[] {t1, t2} );
    }
    
}
