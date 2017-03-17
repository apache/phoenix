package org.apache.phoenix.coprocessor;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.end2end.AlterMultiTenantTableWithViewsIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class MetaDataEndpointImplTest extends ParallelStatsDisabledIT {

    private final boolean isMultiTenant = false;

    private final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant1";
    private final String TENANT_SPECIFIC_URL2 = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant2";

    private String generateDDL(String format) {
        return String
            .format(format, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "", isMultiTenant ? "TENANT_ID, " : "",
                isMultiTenant ? "MULTI_TENANT=true" : "");
    }

    @Test
    public void testAddNewColumnsToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
            Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn) {
            String tableName = generateUniqueName();
            String viewOfTable = tableName + "_VIEW";
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + " %s ID char(1) NOT NULL,"
                + " COL1 integer NOT NULL," + " COL2 bigint NOT NULL,"
                + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 3,
                QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");

            viewConn.createStatement().execute(
                "CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + tableName);
            assertTableDefinition(conn, viewOfTable, PTableType.VIEW, tableName, 0, 5, 3, "ID", "COL1", "COL2",
                "VIEW_COL1", "VIEW_COL2");

            PTable viewPTable = PhoenixRuntime.getTable(viewConn, viewOfTable.toUpperCase());
            PTableProtos.PTable protoObject = PTableImpl.toProto(viewPTable);
            int serializedSize = protoObject.toByteArray().length;
            System.out.println("serializedSize = " + serializedSize);
        }
    }


    @Test
    public void testMetaDataEndpointParentAndChildren() throws Exception {
        TableName catalogTable = TableName.valueOf("SYSTEM.CATALOG");
        String baseTable = "BASE_TABLE";
        String childView = "FIRST_VIEW";
        String grandChildView = "SECOND_VIEW";
        String greatGrandChildView = "THIRD_VIEW";
        Connection conn = DriverManager.getConnection(getUrl());
        Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn;
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)" + " ) %s";
        conn.createStatement().execute(generateDDL(ddlFormat));

        String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
        viewConn.createStatement().execute(childViewDDL);

        String grandChildViewDDL = "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
        viewConn.createStatement().execute(grandChildViewDDL);

        String greatGrandChildViewDDL = "CREATE VIEW " + greatGrandChildView + " AS SELECT * FROM " + grandChildView;
        viewConn.createStatement().execute(greatGrandChildViewDDL);

        HRegionInfo regionInfo = Iterables.getOnlyElement(utility.getHBaseAdmin().getTableRegions(catalogTable));

        HTableDescriptor tableDescriptor = mock(HTableDescriptor.class);
        when(tableDescriptor.getTableName()).thenReturn(catalogTable);
        Region region = mock(Region.class);
        when(region.getTableDesc()).thenReturn(tableDescriptor);
        when(region.getRegionInfo()).thenReturn(regionInfo);

        ViewFinder viewFinder = new ViewFinder();
        HTable hTable = new HTable(utility.getConfiguration(), catalogTable);
        PTable table = PhoenixRuntime.getTable(viewConn, baseTable.toUpperCase());

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SYSTEM.CATALOG");
        printResultSet(rs);
        List<Result> childViews = viewFinder.findChildViews(hTable, HConstants.EMPTY_BYTE_ARRAY, table.getSchemaName().getBytes(), table.getTableName().getBytes());
        //        assertEquals(2, childViews.size());

        System.out.println("CHILD VIEWS");
        for (Result view : childViews) {
            System.out.println(view);
        }
        System.out.println("DONE");

        PTable childMostView = PhoenixRuntime.getTable(viewConn, greatGrandChildView.toUpperCase());
        List<Result> parentViews = viewFinder.findParentViews(hTable, HConstants.EMPTY_BYTE_ARRAY, childMostView.getSchemaName().getBytes(), childMostView.getTableName().getBytes());
        System.out.println("PARENT VIEWS");
        for (Result view : parentViews) {
            System.out.println(view);
        }
        System.out.println("DONE");

    }


    private void printResultSet(ResultSet rs) throws Exception {
        ResultSetMetaData rsmd = rs.getMetaData();
        System.out.println("querying SELECT * FROM XXX");
        int columnsNumber = rsmd.getColumnCount();
        boolean firstTime = true;
        List<String> header = Lists.newArrayList();
        while (rs.next()) {
            List<String> values = Lists.newArrayList();
            for (int i = 1; i <= columnsNumber; i++) {
                if (firstTime) {
                    header.add(rsmd.getColumnName(i));
                }
                values.add(rs.getString(i));
            }
            if (firstTime) {
                System.out.println(Joiner.on(", ").useForNull("\"\"").join(header));
                firstTime = false;
            }
            System.out.println(Joiner.on(", ").useForNull("\"\"").join(values));
        }
    }

    public void assertTableDefinition(Connection conn, String tableName, PTableType tableType, String parentTableName,
        int sequenceNumber, int columnCount, int baseColumnCount, String... columnNames) throws Exception {
        int delta = isMultiTenant ? 1 : 0;
        String[] cols;
        if (isMultiTenant) {
            cols = (String[]) ArrayUtils.addAll(new String[] { "TENANT_ID" }, columnNames);
        } else {
            cols = columnNames;
        }
        AlterMultiTenantTableWithViewsIT
            .assertTableDefinition(conn, tableName, tableType, parentTableName, sequenceNumber, columnCount + delta,
                baseColumnCount == QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT ?
                    baseColumnCount :
                    baseColumnCount + delta, cols);
    }

}