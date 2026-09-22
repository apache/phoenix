/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

/** Tests for parsing VECTOR column definitions in SQL statements. */
public class VectorColumnParseTest {

  @Test
  public void testParseVectorFloatColumn() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 128))";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateTableStatement", stmt instanceof CreateTableStatement);
    CreateTableStatement createStmt = (CreateTableStatement) stmt;

    List<ColumnDef> colDefs = createStmt.getColumnDefs();
    assertEquals(2, colDefs.size());

    ColumnDef vectorCol = colDefs.get(1);
    assertEquals("V", vectorCol.getColumnDefName().getColumnName());
    assertEquals(PVectorFloat.INSTANCE, vectorCol.getDataType());
    assertEquals(Integer.valueOf(128), vectorCol.getMaxLength());
  }

  @Test
  public void testParseVectorDoubleColumn() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(DOUBLE, 64))";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateTableStatement", stmt instanceof CreateTableStatement);
    CreateTableStatement createStmt = (CreateTableStatement) stmt;

    List<ColumnDef> colDefs = createStmt.getColumnDefs();
    assertEquals(2, colDefs.size());

    ColumnDef vectorCol = colDefs.get(1);
    assertEquals("V", vectorCol.getColumnDefName().getColumnName());
    assertEquals(PVectorDouble.INSTANCE, vectorCol.getDataType());
    assertEquals(Integer.valueOf(64), vectorCol.getMaxLength());
  }

  @Test
  public void testParseVectorColumnWithNotNull() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 256) NOT NULL)";
    SQLParser parser = new SQLParser(ddl);
    CreateTableStatement createStmt = (CreateTableStatement) parser.parseStatement();

    ColumnDef vectorCol = createStmt.getColumnDefs().get(1);
    assertEquals(PVectorFloat.INSTANCE, vectorCol.getDataType());
    assertEquals(Integer.valueOf(256), vectorCol.getMaxLength());
    assertFalse("Column should be NOT NULL", vectorCol.isNull());
  }

  @Test
  public void testZeroDimensionRejection() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, 0))";
    try {
      SQLParser parser = new SQLParser(ddl);
      parser.parseStatement();
      fail("Should have rejected zero dimension vector column definition");
    } catch (SQLException e) {
      assertEquals("Expected NONPOSITIVE_MAX_LENGTH error code",
        SQLExceptionCode.NONPOSITIVE_MAX_LENGTH.getErrorCode(), e.getErrorCode());
    }
  }

  @Test
  public void testMissingDimensionRejection() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT))";
    try {
      SQLParser parser = new SQLParser(ddl);
      parser.parseStatement();
      fail("Should have rejected missing dimension vector column definition");
    } catch (SQLException e) {
      // Expected parse error due to missing dimension
      assertNotNull("Expected parse exception", e.getMessage());
    }
  }

  @Test
  public void testNegativeDimensionRejection() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(FLOAT, -5))";
    try {
      SQLParser parser = new SQLParser(ddl);
      parser.parseStatement();
      fail("Should have rejected negative dimension vector column definition");
    } catch (SQLException e) {
      assertNotNull("Expected parse exception for negative dimension", e.getMessage());
    }
  }

  @Test
  public void testInvalidComponentTypeRejection() throws Exception {
    String ddl = "CREATE TABLE t (pk INTEGER PRIMARY KEY, v VECTOR(INT, 128))";
    try {
      SQLParser parser = new SQLParser(ddl);
      parser.parseStatement();
      fail("Should have rejected unsupported component type in vector column definition");
    } catch (SQLException e) {
      assertNotNull("Expected parse exception for invalid component type", e.getMessage());
    }
  }

  @Test
  public void testDynamicVectorColumn() throws Exception {
    String sql = "SELECT * FROM t(v VECTOR(FLOAT, 128))";
    SQLParser parser = new SQLParser(sql);
    SelectStatement select = (SelectStatement) parser.parseStatement();
    NamedTableNode tableNode = (NamedTableNode) select.getFrom();
    List<ColumnDef> dynCols = tableNode.getDynamicColumns();
    assertEquals(1, dynCols.size());
    ColumnDef dynCol = dynCols.get(0);
    assertEquals("V", dynCol.getColumnDefName().getColumnName());
    assertEquals(PVectorFloat.INSTANCE, dynCol.getDataType());
    assertEquals(Integer.valueOf(128), dynCol.getMaxLength());
  }

  @Test
  public void testColumnDefConstructorValidation() {
    ColumnName colName = new ColumnName("V");
    try {
      new ColumnDef(colName, PVectorFloat.INSTANCE, null, null, null, false, SortOrder.getDefault(),
        null, null, false);
      fail("Should reject null dimension for vector type");
    } catch (ParseException e) {
      assertTrue("Cause must be SQLException", e.getCause() instanceof SQLException);
      assertEquals(SQLExceptionCode.MISSING_MAX_LENGTH.getErrorCode(),
        ((SQLException) e.getCause()).getErrorCode());
    }

    try {
      new ColumnDef(colName, PVectorFloat.INSTANCE, null, 0, null, false, SortOrder.getDefault(),
        null, null, false);
      fail("Should reject zero dimension for vector type");
    } catch (ParseException e) {
      assertTrue("Cause must be SQLException", e.getCause() instanceof SQLException);
      assertEquals(SQLExceptionCode.NONPOSITIVE_MAX_LENGTH.getErrorCode(),
        ((SQLException) e.getCause()).getErrorCode());
    }

    try {
      new ColumnDef(colName, PVectorDouble.INSTANCE, null, -1, null, false, SortOrder.getDefault(),
        null, null, false);
      fail("Should reject negative dimension for vector type");
    } catch (ParseException e) {
      assertTrue("Cause must be SQLException", e.getCause() instanceof SQLException);
      assertEquals(SQLExceptionCode.NONPOSITIVE_MAX_LENGTH.getErrorCode(),
        ((SQLException) e.getCause()).getErrorCode());
    }
  }

  @Test
  public void testColumnDefToString() {
    ColumnDef floatVec = new ColumnDef(new ColumnName("V"), PVectorFloat.INSTANCE, null, 128, null,
      false, SortOrder.getDefault(), null, null, false);
    assertEquals("V VECTOR(FLOAT, 128)", floatVec.toString());

    ColumnDef doubleVec = new ColumnDef(new ColumnName("V"), PVectorDouble.INSTANCE, null, 64, null,
      false, SortOrder.getDefault(), null, null, false);
    assertEquals("V VECTOR(DOUBLE, 64)", doubleVec.toString());
  }

  @Test
  public void testParseVectorIndexFullSyntax() throws Exception {
    String ddl =
      "CREATE VECTOR INDEX idx ON t (v) WITH (metric='COSINE', algorithm='IVF', lists=32, sample_size=1000)";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    assertEquals("IDX", indexStmt.getIndexTableName().getTableName());
    assertEquals("T", indexStmt.getTable().getName().getTableName());
    assertEquals(PTable.IndexType.VECTOR_GLOBAL, indexStmt.getIndexType());
    assertFalse("Expected ifNotExists to be false", indexStmt.ifNotExists());
    assertFalse("Expected async to be false", indexStmt.isAsync());
    assertEquals(1, indexStmt.getIndexConstraint().getParseNodeAndSortOrderList().size());

    List<Pair<String, Object>> props =
      indexStmt.getProps().get(QueryConstants.ALL_FAMILY_PROPERTIES_KEY);
    assertEquals(4, props.size());
    boolean foundMetric = false, foundAlgorithm = false, foundLists = false,
        foundSampleSize = false;
    for (Pair<String, Object> prop : props) {
      if ("METRIC".equals(prop.getFirst())) {
        foundMetric = true;
        assertEquals("COSINE", prop.getSecond());
      } else if ("ALGORITHM".equals(prop.getFirst())) {
        foundAlgorithm = true;
        assertEquals("IVF", prop.getSecond());
      } else if ("LISTS".equals(prop.getFirst())) {
        foundLists = true;
        assertEquals(32, ((Number) prop.getSecond()).intValue());
      } else if ("SAMPLE_SIZE".equals(prop.getFirst())) {
        foundSampleSize = true;
        assertEquals(1000, ((Number) prop.getSecond()).intValue());
      }
    }
    assertTrue("METRIC property expected", foundMetric);
    assertTrue("ALGORITHM property expected", foundAlgorithm);
    assertTrue("LISTS property expected", foundLists);
    assertTrue("SAMPLE_SIZE property expected", foundSampleSize);
  }

  @Test
  public void testParseVectorIndexIfNotExists() throws Exception {
    String ddl =
      "CREATE VECTOR INDEX IF NOT EXISTS idx ON t (v) WITH (algorithm='IVF', metric='L2', lists=16, sample_size=500)";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    assertTrue("Expected ifNotExists to be true", indexStmt.ifNotExists());
    assertEquals(PTable.IndexType.VECTOR_GLOBAL, indexStmt.getIndexType());
  }

  @Test
  public void testParseVectorIndexAsync() throws Exception {
    String ddl =
      "CREATE VECTOR INDEX idx ON t (v) WITH (algorithm='IVF', metric='L2', lists=16, sample_size=500) ASYNC";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    assertTrue("Expected async to be true", indexStmt.isAsync());
    assertEquals(PTable.IndexType.VECTOR_GLOBAL, indexStmt.getIndexType());
  }

  @Test
  public void testParseVectorIndexIncludeClause() throws Exception {
    String ddl =
      "CREATE VECTOR INDEX idx ON t (v) INCLUDE (col1, col2) WITH (metric='COSINE', algorithm='IVF', lists=32, sample_size=1000)";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    List<ColumnName> includeCols = indexStmt.getIncludeColumns();
    assertEquals(2, includeCols.size());
    assertEquals("COL1", includeCols.get(0).getColumnName());
    assertEquals("COL2", includeCols.get(1).getColumnName());
  }

  @Test
  public void testParseVectorIndexFunctionExpression() throws Exception {
    String ddl =
      "CREATE VECTOR INDEX idx ON t (BSON_VECTOR_VALUE(profile, 'search.embedding', 768)) "
        + "INCLUDE (author) WITH (metric='COSINE', algorithm='IVF', lists=1024, sample_size=50000)";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    assertEquals(1, indexStmt.getIndexConstraint().getParseNodeAndSortOrderList().size());
    assertTrue(indexStmt.getIndexConstraint().getParseNodeAndSortOrderList().get(0)
      .getFirst() instanceof FunctionParseNode);
    FunctionParseNode fn = (FunctionParseNode) indexStmt.getIndexConstraint()
      .getParseNodeAndSortOrderList().get(0).getFirst();
    assertEquals("BSON_VECTOR_VALUE", fn.getName());
    assertEquals(1, indexStmt.getIncludeColumns().size());
    assertEquals("AUTHOR", indexStmt.getIncludeColumns().get(0).getColumnName());
  }

  @Test
  public void testVectorIndexAccessorValues() throws Exception {
    String ddl =
      "CREATE VECTOR INDEX idx ON t (v) WITH (algorithm='IVF', metric='COSINE', lists=32, sample_size=1000)";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    assertEquals("IVF", indexStmt.getVectorAlgorithm());
    assertEquals("COSINE", indexStmt.getVectorMetric());
    assertEquals(Integer.valueOf(32), indexStmt.getVectorLists());
    assertEquals(Integer.valueOf(1000), indexStmt.getVectorSampleSize());
  }

  @Test
  public void testCreateIndexStatementConstructorOverload() {
    CreateIndexStatement stmt = new CreateIndexStatement(new NamedNode("IDX"),
      new NamedTableNode(null, TableName.create(null, "T")), IndexKeyConstraint.EMPTY, null, null,
      null, false, PTable.IndexType.VECTOR_GLOBAL, false, 0, null, null, "IVF", "L2", 64, 2000);
    assertEquals("IVF", stmt.getVectorAlgorithm());
    assertEquals("L2", stmt.getVectorMetric());
    assertEquals(Integer.valueOf(64), stmt.getVectorLists());
    assertEquals(Integer.valueOf(2000), stmt.getVectorSampleSize());
  }

  @Test
  public void testNonVectorIndexAccessorValues() throws Exception {
    String ddl = "CREATE INDEX idx ON t (v)";
    SQLParser parser = new SQLParser(ddl);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected CreateIndexStatement", stmt instanceof CreateIndexStatement);
    CreateIndexStatement indexStmt = (CreateIndexStatement) stmt;
    assertNull(indexStmt.getVectorAlgorithm());
    assertNull(indexStmt.getVectorMetric());
    assertNull(indexStmt.getVectorLists());
    assertNull(indexStmt.getVectorSampleSize());
  }

  @Test
  public void testParseBsonVectorValueFunctionInSelect() throws Exception {
    String sql = "SELECT BSON_VECTOR_VALUE(doc, 'path', 128) FROM t";
    SQLParser parser = new SQLParser(sql);
    BindableStatement stmt = parser.parseStatement();
    assertTrue("Expected SelectStatement", stmt instanceof SelectStatement);
    SelectStatement selectStmt = (SelectStatement) stmt;
    assertEquals(1, selectStmt.getSelect().size());
    ParseNode node = selectStmt.getSelect().get(0).getNode();
    assertTrue("Expected BsonVectorValueParseNode", node instanceof BsonVectorValueParseNode);
    BsonVectorValueParseNode funcNode = (BsonVectorValueParseNode) node;
    assertEquals("BSON_VECTOR_VALUE", funcNode.getName());
    assertEquals(3, funcNode.getChildren().size());
  }
}
