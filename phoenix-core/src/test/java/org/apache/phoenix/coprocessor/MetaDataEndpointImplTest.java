package org.apache.phoenix.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
    private final TableName catalogTable = TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
    private final TableName linkTable = TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES);

    /*
      The tree structure is as follows: Where ParentTable is the Base Table
      and all children are views and child views respectively.

                ParentTable
                  /     \
            leftChild   rightChild
              /
       leftGrandChild
     */

    @Test
    public void testGettingChildrenAndParentViews() throws Exception {
        String baseTable = generateUniqueName();
        String leftChild = generateUniqueName();
        String rightChild = generateUniqueName();
        String leftGrandChild = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);

        conn.createStatement().execute("CREATE VIEW " + rightChild + " AS SELECT * FROM " + baseTable);
        conn.createStatement().execute("CREATE VIEW " + leftChild + " (carrier VARCHAR) AS SELECT * FROM " + baseTable);
        conn.createStatement().execute("CREATE VIEW " + leftGrandChild + " (dropped_calls BIGINT) AS SELECT * FROM " + leftChild);

        PTable table = PhoenixRuntime.getTable(conn, baseTable.toUpperCase());
        PTable rightChildTable = PhoenixRuntime.getTable(conn, rightChild.toUpperCase());
        System.err.println(rightChildTable);

        TableViewFinderResult childViews = new TableViewFinderResult();
        ViewFinder.findAllRelatives(getTable(linkTable), HConstants.EMPTY_BYTE_ARRAY, table.getSchemaName().getBytes(),
            table.getTableName().getBytes(), PTable.LinkType.CHILD_TABLE, childViews);
        assertEquals(3, childViews.getLinks().size());

        PTable childMostView = PhoenixRuntime.getTable(conn , leftGrandChild.toUpperCase());
        TableViewFinderResult parentViews = new TableViewFinderResult();
        ViewFinder
            .findAllRelatives(getTable(catalogTable), HConstants.EMPTY_BYTE_ARRAY, childMostView.getSchemaName().getBytes(),
                childMostView.getTableName().getBytes(), PTable.LinkType.PARENT_TABLE, parentViews);
        // returns back everything but the parent table - should only return back the left_child and not the right child
        assertEquals(1, parentViews.getLinks().size());
        // now lets check and make sure the columns are correct
        assertColumnNamesEqual(PhoenixRuntime.getTable(conn, childMostView.getName().getString()), "PK2", "V1", "V2", "CARRIER", "DROPPED_CALLS");

    }

    @Test
    public void testGettingOneChild() throws Exception {
        String baseTable = generateUniqueName();
        String leftChild = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + leftChild + " (carrier VARCHAR) AS SELECT * FROM " + baseTable);


        // now lets check and make sure the columns are correct
        assertColumnNamesEqual(PhoenixRuntime.getTable(conn, leftChild.toUpperCase()), "PK2", "V1", "V2", "CARRIER");
    }

    @Test
    public void testDroppingADerivedColumn() throws Exception {
        String baseTable = generateUniqueName();
        String childView = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat = "CREATE TABLE " + baseTable + " (A VARCHAR PRIMARY KEY, B VARCHAR, C VARCHAR)";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + childView + " (D VARCHAR) AS SELECT * FROM " + baseTable);
        assertColumnNamesEqual(PhoenixRuntime.getTable(conn, childView.toUpperCase()), "A", "B", "C", "D");
        conn.createStatement().execute("ALTER VIEW " + childView + " DROP COLUMN C");

        // now lets check and make sure the columns are correct
        assertColumnNamesEqual(PhoenixRuntime.getTableNoCache(conn, childView.toUpperCase()), "A", "B", "D");

    }

    @Test
    public void testDroppingAColumn() throws Exception {
        String baseTable = generateUniqueName();
        String childView = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat = "CREATE TABLE " + baseTable + " (A VARCHAR PRIMARY KEY, B VARCHAR, C VARCHAR)";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + childView + " (D VARCHAR) AS SELECT * FROM " + baseTable);
        assertColumnNamesEqual(PhoenixRuntime.getTable(conn, childView.toUpperCase()), "A", "B", "C", "D");
        conn.createStatement().execute("ALTER TABLE " + baseTable + " DROP COLUMN C");

        // now lets check and make sure the columns are correct
        assertColumnNamesEqual(PhoenixRuntime.getTableNoCache(conn, childView.toUpperCase()), "A", "B", "D");
    }

    @Test
    public void testAlteringBaseColumns() throws Exception {
        String baseTable = generateUniqueName();
        String leftChild = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + leftChild + " (carrier VARCHAR) AS SELECT * FROM " + baseTable);

        // now lets check and make sure the columns are correct
        PTable childPTable = PhoenixRuntime.getTable(conn, leftChild.toUpperCase());
        assertColumnNamesEqual(childPTable, "PK2", "V1", "V2", "CARRIER");

        // now lets alter the base table by adding a column
        conn.createStatement().execute("ALTER TABLE " + baseTable + " ADD V3 integer");

        // make sure that column was added to the base table
        PTable table = PhoenixRuntime.getTableNoCache(conn, baseTable.toUpperCase());
        assertColumnNamesEqual(table, "PK2", "V1", "V2", "V3");


        childPTable = PhoenixRuntime.getTableNoCache(conn, leftChild.toUpperCase());
        assertColumnNamesEqual(childPTable, "PK2", "V1", "V2", "V3", "CARRIER");
    }

    @Test
    public void testAddingAColumnWithADifferentDefinition() throws Exception {
        String baseTable = generateUniqueName();
        String view = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + view + " (carrier BIGINT) AS SELECT * FROM " + baseTable);
        Map<String, String> expected = new ImmutableMap.Builder<String, String>()
            .put("PK2", "VARCHAR")
            .put("V1", "VARCHAR")
            .put("V2", "VARCHAR")
            .put("CARRIER", "BIGINT")
            .build();

        assertColumnNamesAndDefinitionsEqual(PhoenixRuntime.getTable(conn , view.toUpperCase()), expected);
        try {
            conn.createStatement().execute("ALTER TABLE " + baseTable + " ADD carrier VARCHAR");
        }
        catch(SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }

        Map<String, String> expectedBaseTableColumns = new ImmutableMap.Builder<String, String>()
            .put("PK2", "VARCHAR")
            .put("V1", "VARCHAR")
            .put("V2", "VARCHAR")
            .build();

        assertColumnNamesAndDefinitionsEqual(PhoenixRuntime.getTable(conn , baseTable.toUpperCase()), expectedBaseTableColumns);

        // the view column "CARRIER" should still be unchanged
        Map<String, String> expectedViewColumnDefinition = new ImmutableMap.Builder<String, String>()
            .put("PK2", "VARCHAR")
            .put("V1", "VARCHAR")
            .put("V2", "VARCHAR")
            .put("CARRIER", "BIGINT")
            .build();

        assertColumnNamesAndDefinitionsEqual(PhoenixRuntime.getTable(conn , view.toUpperCase()), expectedViewColumnDefinition);
    }

    public void testDropCascade() throws Exception {
        String baseTable = generateUniqueName();
        String child = generateUniqueName();
        String grandChild = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + child + " (A VARCHAR) AS SELECT * FROM " + baseTable);
        conn.createStatement().execute("CREATE VIEW " + grandChild + " (B VARCHAR) AS SELECT * FROM " + child);

        PTable childMostView = PhoenixRuntime.getTable(conn , child.toUpperCase());
        // now lets check and make sure the columns are correct
        PTable grandChildPTable = PhoenixRuntime.getTable(conn, childMostView.getName().getString());
        assertColumnNamesEqual(grandChildPTable, "PK2", "V1", "V2", "A");

        // now lets drop the parent table
        conn.createStatement().execute("DROP TABLE " + baseTable + " CASCADE");

        // the tables should no longer exist
        try {
            PhoenixRuntime.getTableNoCache(conn, baseTable);
            fail();
        }
        catch(TableNotFoundException e){}
        try {
            PhoenixRuntime.getTableNoCache(conn, child);
            fail();
        }
        catch(TableNotFoundException e){}
        try {
            PhoenixRuntime.getTableNoCache(conn, grandChild);
            fail();
        }
        catch(TableNotFoundException e){}
    }

    @Test
    public void testWhereClause() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String baseTableName = generateUniqueName();
        String childViewName = generateUniqueName();
        String grandChildViewName = generateUniqueName();
        String baseTableDdl = "CREATE TABLE " + baseTableName + " (" +
            "A0 CHAR(1) NOT NULL PRIMARY KEY," +
            "A1 CHAR(1), A2 CHAR (1))";
        conn.createStatement().execute(baseTableDdl);
        conn.createStatement().execute(
            "CREATE VIEW " + childViewName + " AS SELECT * FROM " + baseTableName + " WHERE A1 = 'X'");
        conn.createStatement().execute(
            "CREATE VIEW " + grandChildViewName + " AS SELECT * FROM " + childViewName + " WHERE A2 = 'Y'");

        PTable childViewTable = PhoenixRuntime.getTableNoCache(conn, childViewName);
        PTable grandChildViewTable = PhoenixRuntime.getTableNoCache(conn, grandChildViewName);

        assertNotNull(childViewTable.getColumnForColumnName("A1").getViewConstant());
        assertNotNull(grandChildViewTable.getColumnForColumnName("A1").getViewConstant());
        assertNotNull(grandChildViewTable.getColumnForColumnName("A2").getViewConstant());
    }

    private void assertColumnNamesEqual(PTable table, String... cols) {
        List<String> actual = Lists.newArrayList();
        for (PColumn column : table.getColumns()) {
            actual.add(column.getName().getString().trim());
        }
        List<String> expected = Arrays.asList(cols);
        assertEquals(Joiner.on(", ").join(expected), Joiner.on(", ").join(actual));
    }

    private void assertColumnNamesAndDefinitionsEqual(PTable table, Map<String, String> expected) {
        Map<String, String> actual = Maps.newHashMap();
        for (PColumn column : table.getColumns()) {
            actual.put(column.getName().getString().trim(), column.getDataType().getSqlTypeName());
        }
        assertEquals(expected, actual);
    }

    private HTable getTable(TableName catalogTable) throws IOException {
        return new HTable(utility.getConfiguration(), catalogTable);
    }

}