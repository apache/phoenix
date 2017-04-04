package org.apache.phoenix.coprocessor;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

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
        String baseTable = "PARENT_TABLE";
        String leftChild = "LEFT_CHILD";
        String rightChild = "RIGHT_CHILD";
        String leftGrandChild = "LEFT_GRANDCHILD";
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
        List<PColumn> columns = grandChildPTable.getColumns();
        List<String> columnNames = Lists.newArrayList();
        List<String> expectedColumnNames = Lists.newArrayList("PK2", "V1", "V2", "CARRIER", "DROPPED_CALLS");
        for (PColumn column : columns) {
            System.out.println("column = " + column);
            columnNames.add(column.getName().getString().trim());
        }
        assertEquals(Joiner.on(", ").join(expectedColumnNames), Joiner.on(", ").join(columnNames));

    }

    @Test
    public void testGettingOneChild() throws Exception {
        TableName catalogTable = TableName.valueOf("SYSTEM.CATALOG");
        String baseTable = "PARENT_TABLE";
        String leftChild = "LEFT_CHILD";
        Connection conn = DriverManager.getConnection(getUrl());
        String ddlFormat =
            "CREATE TABLE IF NOT EXISTS " + baseTable + "  (" + " PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                + " CONSTRAINT NAME_PK PRIMARY KEY (PK2)" + " )";
        conn.createStatement().execute(ddlFormat);
        conn.createStatement().execute("CREATE VIEW " + leftChild + " (carrier VARCHAR) AS SELECT * FROM " + baseTable);

        ResultSet resultset = conn.getMetaData().getColumns("", "", leftChild.toUpperCase(), null);
        int i = 1;
        while (resultset.next()) {
            String column_name = resultset.getString("COLUMN_NAME");
            System.out.println("column_name = " + column_name);
        }

        conn.unwrap(PhoenixConnection.class).removeTable(null, leftChild.toUpperCase(), baseTable.toUpperCase(), HConstants.LATEST_TIMESTAMP);

        PTable childMostView = PhoenixRuntime.getTable(conn , leftChild.toUpperCase());
        // now lets check and make sure the columns are correct
        PTable grandChildPTable = PhoenixRuntime.getTable(conn, childMostView.getName().getString());
        List<PColumn> columns = grandChildPTable.getColumns();
        List<String> columnNames = Lists.newArrayList();
        List<String> expectedColumnNames = Lists.newArrayList("PK2", "V1", "V2", "CARRIER");
        for (PColumn column : columns) {
            System.out.println("column = " + column);
            columnNames.add(column.getName().getString().trim());
        }
        assertEquals(Joiner.on(", ").join(expectedColumnNames), Joiner.on(", ").join(columnNames));

    }

    private HTable getTable(TableName catalogTable) throws IOException {
        return new HTable(utility.getConfiguration(), catalogTable);
    }

}