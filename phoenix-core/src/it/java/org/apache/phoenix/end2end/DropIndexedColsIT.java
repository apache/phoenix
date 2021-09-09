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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class DropIndexedColsIT extends SplitSystemCatalogIT {

  private static final String CREATE_TABLE_COL_QUERY = " (%s k VARCHAR NOT NULL, v1 VARCHAR, " +
      "v2 VARCHAR, v3 VARCHAR, v4 VARCHAR, v5 VARCHAR CONSTRAINT PK PRIMARY KEY(%s k))%s";
  private static final String CREATE_TABLE = "CREATE TABLE ";
  private static final String CREATE_VIEW = "CREATE VIEW ";
  private static final String CREATE_INDEX = "CREATE INDEX ";
  private static final String SELECT_ALL_FROM = "SELECT * FROM ";
  private static final String UPSERT_INTO = "UPSERT INTO ";
  private static final String ALTER_TABLE = "ALTER TABLE ";

  private final boolean salted;
  private final String TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + "=" + TENANT1;

  public DropIndexedColsIT(boolean salted) {
    this.salted = salted;
  }

  @Parameterized.Parameters(name = "DropIndexedColsIT_salted={0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(false, true);
  }

  @Test
  public void testDropIndexedColsMultiTables() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl());
         Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL)) {
      String tableWithView1 = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
      String tableWithView2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

      String viewOfTable1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
      String viewOfTable2 = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());

      String viewSchemaName1 = SchemaUtil.getSchemaNameFromFullName(viewOfTable1);
      String viewSchemaName2 = SchemaUtil.getSchemaNameFromFullName(viewOfTable2);

      String viewIndex1 = generateUniqueName();
      String viewIndex2 = generateUniqueName();
      String viewIndex3 = generateUniqueName();
      String viewIndex4 = generateUniqueName();
      String viewIndex5 = generateUniqueName();

      String fullNameViewIndex1 = SchemaUtil.getTableName(viewSchemaName1, viewIndex1);
      String fullNameViewIndex2 = SchemaUtil.getTableName(viewSchemaName1, viewIndex2);
      String fullNameViewIndex3 = SchemaUtil.getTableName(viewSchemaName2, viewIndex3);
      String fullNameViewIndex4 = SchemaUtil.getTableName(viewSchemaName2, viewIndex4);

      conn.setAutoCommit(false);
      viewConn.setAutoCommit(false);
      String ddlFormat = new StringBuilder()
          .append(CREATE_TABLE)
          .append(tableWithView1)
          .append(CREATE_TABLE_COL_QUERY).toString();
      String ddlFormat2 = new StringBuilder()
          .append(CREATE_TABLE)
          .append(tableWithView2)
          .append(CREATE_TABLE_COL_QUERY).toString();
      conn.createStatement().execute(generateDDL(ddlFormat));
      conn.createStatement().execute(generateDDL(ddlFormat2));
      viewConn.createStatement().execute(new StringBuilder(CREATE_VIEW)
          .append(viewOfTable1)
          .append(" ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM ")
          .append(tableWithView1).toString());
      viewConn.createStatement().execute(new StringBuilder(CREATE_VIEW)
          .append(viewOfTable2)
          .append(" ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM ")
          .append(tableWithView2).toString());

      viewConn.createStatement().execute(CREATE_INDEX + viewIndex1 + " ON " + viewOfTable1
          + "(v2) INCLUDE (v4)");
      viewConn.createStatement().execute(CREATE_INDEX + viewIndex2 + " ON " + viewOfTable1
          + "(v1) INCLUDE (v4)");
      viewConn.createStatement().execute(CREATE_INDEX + viewIndex3 + " ON " + viewOfTable2
          + "(v2) INCLUDE (v4)");
      viewConn.createStatement().execute(CREATE_INDEX + viewIndex4 + " ON " + viewOfTable2
          + "(v1) INCLUDE (v4)");
      viewConn.createStatement().execute(CREATE_INDEX + viewIndex5 + " ON " + viewOfTable2
          + "(v3) INCLUDE (v4)");


      viewConn.createStatement().execute(SELECT_ALL_FROM + fullNameViewIndex1);
      viewConn.createStatement().execute(SELECT_ALL_FROM + fullNameViewIndex2);
      viewConn.createStatement().execute(SELECT_ALL_FROM + fullNameViewIndex3);
      viewConn.createStatement().execute(SELECT_ALL_FROM + fullNameViewIndex4);


      // upsert a single row
      PreparedStatement stmt = viewConn.prepareStatement(UPSERT_INTO + viewOfTable1
          + " VALUES(?,?,?,?,?,?,?,?)");
      stmt.setString(1, "a");
      stmt.setString(2, "b");
      stmt.setString(3, "c");
      stmt.setString(4, "d");
      stmt.setString(5, "e");
      stmt.setString(6, "f");
      stmt.setInt(7, 1);
      stmt.setString(8, "g");
      stmt.execute();
      viewConn.commit();

      stmt = viewConn.prepareStatement(UPSERT_INTO + viewOfTable2
          + " VALUES(?,?,?,?,?,?,?,?)");
      stmt.setString(1, "a");
      stmt.setString(2, "b");
      stmt.setString(3, "c");
      stmt.setString(4, "d");
      stmt.setString(5, "e");
      stmt.setString(6, "f");
      stmt.setInt(7, 1);
      stmt.setString(8, "g");
      stmt.execute();
      viewConn.commit();

      // verify the index was created
      PhoenixConnection pconn = viewConn.unwrap(PhoenixConnection.class);
      PName tenantId = PNameFactory.newName(TENANT1);
      PTable view = pconn.getTable(new PTableKey(tenantId, viewOfTable1));
      PTable viewIndex = pconn.getTable(new PTableKey(tenantId, fullNameViewIndex1));
      Assert.assertNotNull("Can't find view index", viewIndex);
      Assert.assertEquals("Unexpected number of indexes ", 2, view.getIndexes().size());
      Assert.assertEquals("Unexpected index", 3, pconn.getTable(new PTableKey(tenantId,
          viewOfTable2)).getIndexes().size());
      conn.createStatement().execute(ALTER_TABLE + tableWithView1 + " DROP COLUMN v2, v3, v5 ");
      conn.createStatement().execute(ALTER_TABLE + tableWithView2 + " DROP COLUMN " +
          "v1, v2, v3, v5 ");
      // verify columns were dropped
      droppedColumnTest(conn, "v2", tableWithView1);
      droppedColumnTest(conn, "v3", tableWithView1);
      droppedColumnTest(conn, "v5", tableWithView1);
      droppedColumnTest(conn, "v1", tableWithView2);
      droppedColumnTest(conn, "v2", tableWithView2);
      droppedColumnTest(conn, "v3", tableWithView2);
      droppedColumnTest(conn, "v5", tableWithView2);

    }
  }

  @Test
  public void testDropIndexedColsSingleTable() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl());
         Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL)) {
      String tableWithView = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
      String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
      String viewSchemaName = SchemaUtil.getSchemaNameFromFullName(viewOfTable);

      String viewIndex1 = generateUniqueName();
      String viewIndex2 = generateUniqueName();

      String fullNameViewIndex1 = SchemaUtil.getTableName(viewSchemaName, viewIndex1);
      String fullNameViewIndex2 = SchemaUtil.getTableName(viewSchemaName, viewIndex2);

      conn.setAutoCommit(false);
      viewConn.setAutoCommit(false);
      String ddlFormat = new StringBuilder()
          .append(CREATE_TABLE)
          .append(tableWithView)
          .append(CREATE_TABLE_COL_QUERY).toString();
      conn.createStatement().execute(generateDDL(ddlFormat));
      viewConn.createStatement().execute(new StringBuilder(CREATE_VIEW)
          .append(viewOfTable)
          .append(" ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM ")
          .append(tableWithView).toString());

      viewConn.createStatement().execute(CREATE_INDEX + viewIndex1 + " ON " + viewOfTable
          + "(v2) INCLUDE (v4)");
      viewConn.createStatement().execute(CREATE_INDEX + viewIndex2 + " ON " + viewOfTable
          + "(v1) INCLUDE (v4)");

      viewConn.createStatement().execute(SELECT_ALL_FROM + fullNameViewIndex1);
      viewConn.createStatement().execute(SELECT_ALL_FROM + fullNameViewIndex2);

      // upsert a single row
      PreparedStatement stmt = viewConn.prepareStatement(UPSERT_INTO + viewOfTable
          + " VALUES(?,?,?,?,?,?,?,?)");
      stmt.setString(1, "a");
      stmt.setString(2, "b");
      stmt.setString(3, "c");
      stmt.setString(4, "d");
      stmt.setString(5, "e");
      stmt.setString(6, "f");
      stmt.setInt(7, 1);
      stmt.setString(8, "g");
      stmt.execute();
      viewConn.commit();

      // verify the index was created
      PhoenixConnection pconn = viewConn.unwrap(PhoenixConnection.class);
      PName tenantId = PNameFactory.newName(TENANT1);
      conn.createStatement().execute(ALTER_TABLE + tableWithView + " DROP COLUMN v2, v3, v5 ");
      // verify columns were dropped
      droppedColumnTest(conn, "v2", tableWithView);
      droppedColumnTest(conn, "v3", tableWithView);
      droppedColumnTest(conn, "v5", tableWithView);

    }
  }


  private String generateDDL(String format) {
    StringBuilder ddlGeneratorBuilder = new StringBuilder();
    if (ddlGeneratorBuilder.length() != 0) {
      ddlGeneratorBuilder.append(",");
    }
    ddlGeneratorBuilder.append("MULTI_TENANT=true");
    if (salted) {
      if (ddlGeneratorBuilder.length() != 0) {
        ddlGeneratorBuilder.append(",");
      }
      ddlGeneratorBuilder.append("SALT_BUCKETS=4");
    }
    return String.format(format, "TENANT_ID VARCHAR NOT NULL, ", "TENANT_ID, ",
        ddlGeneratorBuilder.toString());
  }

  private void droppedColumnTest(Connection conn, String column, String table)
      throws SQLException {
    try {
      conn.createStatement().execute("SELECT " + column + " FROM " + table);
      Assert.fail("Column should have been dropped");
    } catch (ColumnNotFoundException e) {
      // Empty block
    }
  }

}
