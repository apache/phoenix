package org.apache.phoenix.coprocessor;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        TableName catalogTable = TableName.valueOf("SYSTEM.CATALOG");
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

        TableViewFinderResult childViews = new TableViewFinderResult();
        ViewFinder.findAllRelatives(getTable(catalogTable), HConstants.EMPTY_BYTE_ARRAY, table.getSchemaName().getBytes(),
            table.getTableName().getBytes(), PTable.LinkType.CHILD_TABLE, childViews);
        assertEquals(3, childViews.getResults().size());

        System.out.println("CHILD VIEWS");
        for (Result view : childViews.getResults()) {
            System.out.println(view);
        }
        System.out.println("DONE");

        PTable childMostView = PhoenixRuntime.getTable(conn , leftGrandChild.toUpperCase());
        TableViewFinderResult parentViews = new TableViewFinderResult();
        ViewFinder
            .findAllRelatives(getTable(catalogTable), HConstants.EMPTY_BYTE_ARRAY, childMostView.getSchemaName().getBytes(),
                childMostView.getTableName().getBytes(), PTable.LinkType.PARENT_TABLE, parentViews);
        // returns back everything but the parent table - should only return back the left_child and not the right child
        assertEquals(1, parentViews.getResults().size());
        // now lets check and make sure the columns are correct
        PTable grandChildPTable = PhoenixRuntime.getTable(conn, childMostView.getName().getString());
        assertColumnNamesEqual(grandChildPTable, "PK2", "V1", "V2", "CARRIER", "DROPPED_CALLS");

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
        PTable grandChildPTable = PhoenixRuntime.getTable(conn, leftChild.toUpperCase());
        assertColumnNamesEqual(grandChildPTable, "PK2", "V1", "V2", "CARRIER");
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
        dropTableCache(conn, childView, baseTable);
        assertColumnNamesEqual(PhoenixRuntime.getTable(conn, childView.toUpperCase()), "A", "B", "D");

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
        dropTableCache(conn, childView, baseTable);
        assertColumnNamesEqual(PhoenixRuntime.getTable(conn, childView.toUpperCase()), "A", "B", "D");
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

//        ResultSet resultset = conn.getMetaData().getColumns("", "", leftChild.toUpperCase(), null);
//        int i = 1;
//        while (resultset.next()) {
//            String column_name = resultset.getString("COLUMN_NAME");
//            System.out.println("column_name = " + column_name);
//        }

//        conn.unwrap(PhoenixConnection.class).removeTable(null, leftChild.toUpperCase(), baseTable.toUpperCase(), HConstants.LATEST_TIMESTAMP);

        // now lets check and make sure the columns are correct
        PTable childPTable = PhoenixRuntime.getTable(conn, leftChild.toUpperCase());
        assertColumnNamesEqual(childPTable, "PK2", "V1", "V2", "CARRIER");

        // now lets alter the base table by adding a column
        conn.createStatement().execute("ALTER TABLE " + baseTable + " ADD V3 integer");

        // make sure that column was added to the base table
        PTable table = PhoenixRuntime.getTable(conn, baseTable.toUpperCase());
        assertColumnNamesEqual(table, "PK2", "V1", "V2", "V3");

        // now lets check and make sure the columns are correct
        dropTableCache(conn, leftChild, baseTable);

        childPTable = PhoenixRuntime.getTable(conn, leftChild.toUpperCase());
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
        conn.createStatement().execute("ALTER TABLE " + baseTable + " ADD carrier VARCHAR");

        Map<String, String> expectedBaseTableColumns = new ImmutableMap.Builder<String, String>()
            .put("PK2", "VARCHAR")
            .put("V1", "VARCHAR")
            .put("V2", "VARCHAR")
            .put("CARRIER", "VARCHAR")
            .build();

        assertColumnNamesAndDefinitionsEqual(PhoenixRuntime.getTable(conn , baseTable.toUpperCase()), expectedBaseTableColumns);

        // the view column "CARRIER" should still be a BIGINT since it was created first
        Map<String, String> expectedViewColumnDefinition = new ImmutableMap.Builder<String, String>()
            .put("PK2", "VARCHAR")
            .put("V1", "VARCHAR")
            .put("V2", "VARCHAR")
            .put("CARRIER", "BIGINT")
            .build();

        assertColumnNamesAndDefinitionsEqual(PhoenixRuntime.getTable(conn , view.toUpperCase()), expectedViewColumnDefinition);
    }

    public void testDropCascade() throws Exception {
        String baseTable = "PARENT_TABLE";
        String leftChild = "LEFT_CHILD";
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + leftChild + " (carrier VARCHAR) AS SELECT * FROM " + baseTable);

        PTable childMostView = PhoenixRuntime.getTable(conn , leftChild.toUpperCase());
        // now lets check and make sure the columns are correct
        PTable grandChildPTable = PhoenixRuntime.getTable(conn, childMostView.getName().getString());
        assertColumnNamesEqual(grandChildPTable, "PK2", "V1", "V2", "CARRIER");

        // now lets drop the parent table
        conn.createStatement().execute("DROP TABLE " + baseTable + " CASCADE");

        // TODO: CM how to i drop the cache for a table that has no parent
        childMostView = PhoenixRuntime.getTable(conn , leftChild.toUpperCase());
        // now lets check and make sure the columns are correct
        grandChildPTable = PhoenixRuntime.getTable(conn, childMostView.getName().getString());
        assertColumnNamesEqual(grandChildPTable, "PK2", "V1", "V2", "CARRIER");

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
        assertEquals(Joiner.on(", ").withKeyValueSeparator(" => ").join(expected), Joiner.on(", ").withKeyValueSeparator(" => ").join(actual));
    }

    private void dropTableCache(Connection conn, String child, String baseTable) throws SQLException {
        conn.unwrap(PhoenixConnection.class).removeTable(null, child.toUpperCase(), baseTable.toUpperCase(), HConstants.LATEST_TIMESTAMP);
    }

    private HTable getTable(TableName catalogTable) throws IOException {
        return new HTable(utility.getConfiguration(), catalogTable);
    }

}