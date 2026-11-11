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
package org.apache.phoenix.coprocessor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.function.ArrayLengthFunction;
import org.apache.phoenix.expression.function.PartitionIdFunction;
import org.apache.phoenix.expression.function.RawRowSizeFunction;
import org.apache.phoenix.expression.function.RowKeyBytesStringFunction;
import org.apache.phoenix.expression.function.RowSizeFunction;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the early data row filter of uncovered index scans. The tests compile real queries on an
 * uncovered index. They compare the rewritten conjuncts on the data row with the original conjuncts
 * on the joined row that the scanner builds today.
 */
public class UncoveredIndexDataRowFilterTest extends BaseConnectionlessQueryTest {
  private static final byte[] INDEX_ROW = Bytes.toBytes("index-row");
  private static final byte[] DATA_ROW = Bytes.toBytes("data-row");
  private static final long TS = 1000L;

  private static final String COLUMNS = "K VARCHAR PRIMARY KEY, IX VARCHAR, VC VARCHAR, CH CHAR(3),"
    + " I INTEGER, BI BIGINT, SI SMALLINT, TI TINYINT, UI UNSIGNED_INT, DC DECIMAL(10,2),"
    + " DB DOUBLE, FL FLOAT, BO BOOLEAN, DT DATE, TM TIME, TS TIMESTAMP, VB VARBINARY,"
    + " BN BINARY(4), IA INTEGER ARRAY, J BSON";

  private String table;
  private String index;

  /** A compiled uncovered index query with the server copies of its filter and projector. */
  private static final class Compiled {
    private final Scan scan;
    private final Expression where;
    private final TupleProjector projector;
    private final PTable dataTable;

    private Compiled(Scan scan, Expression where, TupleProjector projector, PTable dataTable) {
      this.scan = scan;
      this.where = where;
      this.projector = projector;
      this.dataTable = dataTable;
    }
  }

  @Before
  public void createTables() throws SQLException {
    table = generateUniqueName();
    index = generateUniqueName();
    try (java.sql.Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + table + " (" + COLUMNS + ")");
      stmt.execute("CREATE UNCOVERED INDEX " + index + " ON " + table + " (IX)");
    }
  }

  @Test
  public void testValueEquivalenceForEachDataType() throws Exception {
    String where = "VC = 'abc' AND CH = 'xyz' AND I = 5 AND BI > 10 AND SI = 2 AND TI = 3"
      + " AND UI = 7 AND DC = 1.25 AND DB < 2.5 AND FL > 1.0 AND BO = TRUE"
      + " AND DT > TO_DATE('2020-01-01 00:00:00') AND TM > TO_TIME('2020-01-01 00:00:00')"
      + " AND TS > TO_TIMESTAMP('2020-01-01 00:00:00') AND VB IS NOT NULL AND BN IS NOT NULL"
      + " AND IA IS NOT NULL AND BSON_VALUE(J, 'a', 'VARCHAR') = 'x'";
    Compiled compiled = compile(where);
    assertEquals(18, UncoveredIndexDataRowFilter.getConjuncts(compiled.where).size());
    long future = 1893456000000L;
    Map<String, Object> match = new HashMap<>();
    match.put("VC", "abc");
    match.put("CH", "xyz");
    match.put("I", 5);
    match.put("BI", 11L);
    match.put("SI", (short) 2);
    match.put("TI", (byte) 3);
    match.put("UI", 7);
    match.put("DC", new BigDecimal("1.25"));
    match.put("DB", 1.5d);
    match.put("FL", 2.0f);
    match.put("BO", Boolean.TRUE);
    match.put("DT", new Date(future));
    match.put("TM", new Time(future));
    match.put("TS", new Timestamp(future));
    match.put("VB", new byte[] { 1, 2, 3 });
    match.put("BN", new byte[] { 4, 5, 6, 7 });
    match.put("IA", PDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[] { 1, 2 }));
    match.put("J", new BsonDocument("a", new BsonString("x")));
    Map<String, Object> mismatch = new HashMap<>(match);
    mismatch.put("VC", "abd");
    mismatch.put("CH", "xyy");
    mismatch.put("I", 6);
    mismatch.put("BI", 3L);
    mismatch.put("SI", (short) 9);
    mismatch.put("TI", (byte) 9);
    mismatch.put("UI", 8);
    mismatch.put("DC", new BigDecimal("1.50"));
    mismatch.put("DB", 3.5d);
    mismatch.put("FL", 0.5f);
    mismatch.put("BO", Boolean.FALSE);
    mismatch.put("DT", new Date(0));
    mismatch.put("TM", new Time(0));
    mismatch.put("TS", new Timestamp(0));
    mismatch.put("J", new BsonDocument("a", new BsonString("y")));
    assertEquivalent(compiled, match, true);
    assertEquivalent(compiled, mismatch, false);
    assertEquivalent(compiled, Collections.emptyMap(), false);
    // One wrong value is enough to reject the row.
    for (String column : mismatch.keySet()) {
      Map<String, Object> row = new HashMap<>(match);
      row.put(column, mismatch.get(column));
      if (!Arrays.asList("VB", "BN", "IA").contains(column)) {
        assertEquivalent(compiled, row, false);
      }
    }
  }

  @Test
  public void testValueEquivalenceForMissingCells() throws Exception {
    Map<String, Object> five = Collections.singletonMap("I", 5);
    Map<String, Object> six = Collections.singletonMap("I", 6);
    Map<String, Object> none = Collections.emptyMap();
    Map<String, Object> otherPath =
      Collections.singletonMap("J", new BsonDocument("b", new BsonString("x")));
    String[] wheres = { "I IS NULL", "I IS NOT NULL", "I = 5 OR I IS NULL", "NOT (I = 5)",
      "COALESCE(I, 0) = 0", "CASE WHEN I IS NULL THEN 1 ELSE 2 END = 1", "I IN (1, 5, 9)",
      "I + 1 = 6", "I < BI OR I = 5", "VC LIKE 'ab%'", "BSON_VALUE(J, 'a', 'VARCHAR') = 'x'",
      "BSON_VALUE(J, 'a', 'VARCHAR') IS NULL" };
    for (String where : wheres) {
      Compiled compiled = compile(where);
      for (Map<String, Object> row : Arrays.asList(five, six, none, otherPath)) {
        assertEquivalent(compiled, row, null);
      }
    }
  }

  @Test
  public void testDataColumnReadKeepsNullUnderArrayFunction() throws Exception {
    // Array functions are not on the list of allowed nodes. The read of the data column must
    // still give the same input to a function that treats NULL in a special way.
    Compiled compiled = compile("IA IS NOT NULL");
    ProjectedColumnExpression column = findProjectedColumns(compiled.where).get(0);
    Expression read = UncoveredIndexDataRowFilter.rewrite(column, compiled.projector);
    assertTrue(read instanceof UncoveredIndexDataRowFilter.DataColumnExpression);
    Expression original = new ArrayLengthFunction(Collections.singletonList(column));
    Expression rewritten = new ArrayLengthFunction(Collections.singletonList(read));
    Map<String, Object> present = Collections.singletonMap("IA",
      PDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[] { 1, 2, 3 }));
    for (Map<String, Object> values : Arrays.asList(present,
      Collections.<String, Object> emptyMap())) {
      Result dataRow = dataRow(compiled.dataTable, values);
      assertSameValue(original, joinedTuple(compiled, dataRow), rewritten,
        new ResultTuple(dataRow));
    }
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(compile("ARRAY_LENGTH(IA) = 2").where,
      compiled.projector));
  }

  @Test
  public void testClassifierAcceptsDataOnlyConjuncts() throws Exception {
    Compiled compiled = compile("I = 5 AND (BI = 6 OR VC = 'x')");
    List<Expression> conjuncts = UncoveredIndexDataRowFilter.getConjuncts(compiled.where);
    assertEquals(2, conjuncts.size());
    for (Expression conjunct : conjuncts) {
      assertNotNull(conjunct.toString(),
        UncoveredIndexDataRowFilter.rewriteConjunct(conjunct, compiled.projector));
    }
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    assertEquals(2, filter.getExpression().getChildren().size());
  }

  @Test
  public void testClassifierRejectsIndexColumnsAndKeepsOtherConjuncts() throws Exception {
    // The second conjunct reads an index column, and the third mixes data and index columns.
    Compiled compiled = compile("I = 5 AND IX LIKE '%b' AND (BI = 6 OR IX LIKE '%c')");
    List<Expression> conjuncts = UncoveredIndexDataRowFilter.getConjuncts(compiled.where);
    assertEquals(3, conjuncts.size());
    assertNotNull(
      UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(0), compiled.projector));
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(1), compiled.projector));
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(2), compiled.projector));
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    assertTrue(filter.getExpression() instanceof ComparisonExpression);
    // The filter holds only a prefix of the WHERE. FALSE rejects the row, but NULL keeps it.
    assertTrue(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 6))));
    assertFalse(filter.rejects(dataRow(compiled.dataTable, Collections.emptyMap())));
    assertFalse(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 5))));
  }

  @Test
  public void testClassifierStopsAtFirstUnrewritableConjunct() throws Exception {
    // The post-join WHERE evaluates the division first and raises its error on each row. The
    // filter must not reject a row before the WHERE sees it.
    Compiled compiled = compile("10 / TO_NUMBER(IX) = 1 AND I = 5");
    List<Expression> conjuncts = UncoveredIndexDataRowFilter.getConjuncts(compiled.where);
    assertEquals(2, conjuncts.size());
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(0), compiled.projector));
    assertNotNull(
      UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(1), compiled.projector));
    assertNull(
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector));
  }

  @Test
  public void testFilterKeepsNullPrefixBeforeUnrewritableConjunct() throws Exception {
    // A FALSE prefix stops the post-join WHERE before the division. A NULL prefix does not, so
    // the WHERE still evaluates the division and raises its error.
    Compiled compiled = compile("I = 5 AND 10 / TO_NUMBER(IX) = 1");
    List<Expression> conjuncts = UncoveredIndexDataRowFilter.getConjuncts(compiled.where);
    assertEquals(2, conjuncts.size());
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(1), compiled.projector));
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    assertTrue(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 6))));
    assertFalse(filter.rejects(dataRow(compiled.dataTable, Collections.emptyMap())));
    assertFalse(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 5))));
  }

  @Test
  public void testClassifierRejectsIndexOnlyWhere() throws Exception {
    Compiled compiled = compile(table, index, "VC", "IX LIKE '%b'");
    assertNull(
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector));
  }

  @Test
  public void testClassifierRejectsTopLevelOrWithIndexColumn() throws Exception {
    Compiled compiled = compile("I = 5 OR IX LIKE '%b'");
    assertEquals(1, UncoveredIndexDataRowFilter.getConjuncts(compiled.where).size());
    assertNull(
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector));
  }

  @Test
  public void testClassifierRejectsNonDeterministicNodes() throws Exception {
    Compiled compiled = compile("I < RAND() * 10");
    assertNull(
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector));
    // A node that is allowed by type is still rejected when it is not deterministic.
    Compiled equality = compile("I = 5");
    Expression column = findProjectedColumns(equality.where).get(0);
    Expression literal =
      LiteralExpression.newConstant(5, PInteger.INSTANCE, Determinism.PER_INVOCATION);
    Expression comparison =
      ((ComparisonExpression) equality.where).clone(Arrays.asList(column, literal));
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(comparison, equality.projector));
    Expression stable = ((ComparisonExpression) equality.where)
      .clone(Arrays.asList(column, LiteralExpression.newConstant(5, PInteger.INSTANCE)));
    assertNotNull(UncoveredIndexDataRowFilter.rewriteConjunct(stable, equality.projector));
  }

  @Test
  public void testClassifierRejectsTupleReaders() throws Exception {
    // These functions read the row key or the cells of the tuple. The data row gives a different
    // answer than the joined row.
    Compiled compiled =
      compile("PHOENIX_ROW_TIMESTAMP() > TO_DATE('2020-01-01 00:00:00') AND I = 5");
    List<Expression> conjuncts = UncoveredIndexDataRowFilter.getConjuncts(compiled.where);
    assertEquals(2, conjuncts.size());
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(0), compiled.projector));
    assertNotNull(
      UncoveredIndexDataRowFilter.rewriteConjunct(conjuncts.get(1), compiled.projector));
    List<Expression> none = Collections.emptyList();
    Expression[] readers = { new RowKeyBytesStringFunction(none), new RowSizeFunction(none),
      new RawRowSizeFunction(none), new PartitionIdFunction(none) };
    for (Expression reader : readers) {
      Expression conjunct = new IsNullExpression(Collections.singletonList(reader), true);
      assertNull(reader.toString(),
        UncoveredIndexDataRowFilter.rewriteConjunct(conjunct, compiled.projector));
    }
  }

  @Test
  public void testClassifierRejectsTypeOrSortOrderMismatch() throws Exception {
    Compiled compiled = compile("I = 5");
    ProjectedColumnExpression column = findProjectedColumns(compiled.where).get(0);
    Expression[] sources = compiled.projector.getExpressions();
    KeyValueColumnExpression source = (KeyValueColumnExpression) sources[column.getPosition()];
    Field field = compiled.projector.getSchema().getField(column.getPosition());

    Expression[] otherType = sources.clone();
    otherType[column.getPosition()] =
      new KeyValueColumnExpression(datum(PVarchar.INSTANCE, SortOrder.ASC),
        source.getColumnFamily(), source.getColumnQualifier());
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(compiled.where,
      new TupleProjector(compiled.projector.getSchema(), otherType)));

    Expression[] otherOrder = sources.clone();
    otherOrder[column.getPosition()] =
      new KeyValueColumnExpression(datum(field.getDataType(), SortOrder.DESC),
        source.getColumnFamily(), source.getColumnQualifier());
    assertNull(UncoveredIndexDataRowFilter.rewriteConjunct(compiled.where,
      new TupleProjector(compiled.projector.getSchema(), otherOrder)));

    assertNotNull(UncoveredIndexDataRowFilter.rewriteConjunct(compiled.where, compiled.projector));
  }

  @Test
  public void testClassifierAcceptsSingleCellStorage() throws Exception {
    String singleCellTable = generateUniqueName();
    String singleCellIndex = generateUniqueName();
    try (java.sql.Connection conn = DriverManager.getConnection(getUrl());
      Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + singleCellTable
        + " (K VARCHAR PRIMARY KEY, IX VARCHAR, I INTEGER, VC VARCHAR) IMMUTABLE_ROWS=true,"
        + " IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS");
      stmt
        .execute("CREATE UNCOVERED INDEX " + singleCellIndex + " ON " + singleCellTable + " (IX)");
    }
    Compiled compiled = compile(singleCellTable, singleCellIndex, "K", "I = 5 AND VC IS NULL");
    for (Expression source : compiled.projector.getExpressions()) {
      assertTrue(source instanceof SingleCellColumnExpression);
    }
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    assertEquals(2, filter.getExpression().getChildren().size());
    // A data row without the single cell reads NULL for each column.
    Result empty = dataRow(compiled.dataTable, Collections.emptyMap());
    for (Expression conjunct : UncoveredIndexDataRowFilter.getConjuncts(compiled.where)) {
      assertSameValue(conjunct, joinedTuple(compiled, empty),
        UncoveredIndexDataRowFilter.rewriteConjunct(conjunct, compiled.projector),
        new ResultTuple(empty));
    }
    assertTrue(filter.rejects(empty));
  }

  @Test
  public void testPrefixGuardAcceptsStrictPrefix() throws Exception {
    Compiled compiled = compile("I = 5 AND BI > 10");
    KeyValueSchema columnSchema = findProjectedColumns(compiled.where).get(0).getSchema();
    assertEquals(2, columnSchema.getFieldCount());
    // Columns that the plan adds after the WHERE compiles go to the end of the joined row.
    KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
    for (int i = 0; i < columnSchema.getFieldCount(); i++) {
      builder.addField(columnSchema.getField(i));
    }
    builder.addField(datum(PVarchar.INSTANCE, SortOrder.ASC));
    KeyValueSchema longer = builder.build();
    Expression[] sources = Arrays.copyOf(compiled.projector.getExpressions(), 3);
    sources[2] = new KeyValueColumnExpression(datum(PVarchar.INSTANCE, SortOrder.ASC),
      QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("EXTRA"));
    TupleProjector projector = new TupleProjector(longer, sources);
    assertTrue(UncoveredIndexDataRowFilter.isPrefix(columnSchema, longer));
    Compiled extended = new Compiled(compiled.scan, compiled.where, projector, compiled.dataTable);
    Map<String, Object> match = new HashMap<>();
    match.put("I", 5);
    match.put("BI", 11L);
    assertEquivalent(extended, match, true);
    assertEquivalent(extended, Collections.singletonMap("I", 5), false);
  }

  @Test
  public void testPrefixGuardRejectsReorderedSchema() throws Exception {
    Compiled compiled = compile("I = 5 AND BI > 10");
    KeyValueSchema columnSchema = findProjectedColumns(compiled.where).get(0).getSchema();
    KeyValueSchema reordered = new KeyValueSchemaBuilder(0).addField(columnSchema.getField(1))
      .addField(columnSchema.getField(0)).build();
    assertFalse(UncoveredIndexDataRowFilter.isPrefix(columnSchema, reordered));
    Expression[] sources = compiled.projector.getExpressions();
    TupleProjector projector =
      new TupleProjector(reordered, new Expression[] { sources[1], sources[0] });
    assertNull(UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, projector));
  }

  @Test
  public void testPrefixGuardRejectsOtherNullBitSetFormat() throws Exception {
    Compiled compiled = compile("I = 5");
    KeyValueSchema columnSchema = findProjectedColumns(compiled.where).get(0).getSchema();
    KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
    builder.addField(columnSchema.getField(0));
    for (int i = 0; i < 20; i++) {
      builder.addField(datum(PVarchar.INSTANCE, SortOrder.ASC));
    }
    assertFalse(UncoveredIndexDataRowFilter.isPrefix(columnSchema, builder.build()));
    assertFalse(UncoveredIndexDataRowFilter.isPrefix(columnSchema,
      new KeyValueSchemaBuilder(1).addField(columnSchema.getField(0)).build()));
    // A schema without nullable fields has no bit set, so it is not a prefix of one with a bit set.
    KeyValueSchema noBitSet =
      new KeyValueSchemaBuilder(1).addField(columnSchema.getField(0)).build();
    KeyValueSchema withBitSet = new KeyValueSchemaBuilder(1).addField(columnSchema.getField(0))
      .addField(datum(PVarchar.INSTANCE, SortOrder.ASC)).build();
    assertFalse(UncoveredIndexDataRowFilter.isPrefix(noBitSet, withBitSet));
    assertTrue(UncoveredIndexDataRowFilter.isPrefix(noBitSet, noBitSet));
    assertTrue(UncoveredIndexDataRowFilter.isPrefix(columnSchema, columnSchema));
  }

  @Test
  public void testNoProjectedColumnIsLeft() throws Exception {
    Compiled compiled = compile("NOT (CASE WHEN COALESCE(I, 0) IN (1, 5) THEN BI + 1 ELSE 0 END"
      + " > 10) AND VC LIKE 'a%' AND BSON_VALUE(J, 'a', 'VARCHAR') IS NULL");
    for (Expression conjunct : UncoveredIndexDataRowFilter.getConjuncts(compiled.where)) {
      assertTrue(UncoveredIndexDataRowFilter.containsProjectedColumn(conjunct));
      Expression rewritten =
        UncoveredIndexDataRowFilter.rewriteConjunct(conjunct, compiled.projector);
      assertNotNull(conjunct.toString(), rewritten);
      assertFalse(UncoveredIndexDataRowFilter.containsProjectedColumn(rewritten));
    }
    Expression column = findProjectedColumns(compiled.where).get(0);
    assertTrue(UncoveredIndexDataRowFilter.containsProjectedColumn(
      new NotExpression(Collections.singletonList(new NotExpression(column)))));
  }

  @Test
  public void testExceptionOrIncompleteEvaluationKeepsRow() throws Exception {
    Result row = dataRow(compile("I = 5").dataTable, Collections.singletonMap("I", 5));
    assertFalse(new UncoveredIndexDataRowFilter(new TestBoolean() {
      @Override
      public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        throw new IllegalStateException("evaluation failed");
      }
    }).rejects(row));
    assertFalse(new UncoveredIndexDataRowFilter(new TestBoolean() {
      @Override
      public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return false;
      }
    }).rejects(row));
    // A compiled conjunct that fails on the data row also keeps the row.
    Compiled compiled = compile("10 / I = 1");
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    assertFalse(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 0))));
    assertTrue(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 5))));
  }

  @Test
  public void testFilterResetsStateForEachRow() throws Exception {
    Compiled compiled = compile("I = 5 AND BI = 6");
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    // The first row gives NULL for the AND. The second row must not see that NULL.
    assertTrue(filter.rejects(dataRow(compiled.dataTable, Collections.singletonMap("I", 5))));
    Map<String, Object> match = new HashMap<>();
    match.put("I", 5);
    match.put("BI", 6L);
    assertFalse(filter.rejects(dataRow(compiled.dataTable, match)));
  }

  @Test
  public void testNoFilterForAnalyzeScanOrMissingParts() throws Exception {
    Compiled compiled = compile("I = 5");
    assertNotNull(
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector));
    assertNull(UncoveredIndexDataRowFilter.create(compiled.scan, null, compiled.projector));
    assertNull(UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, null));
    Scan analyze = new Scan(compiled.scan);
    analyze.setAttribute(BaseScannerRegionObserverConstants.ANALYZE_TABLE, PDataType.TRUE_BYTES);
    assertNull(UncoveredIndexDataRowFilter.create(analyze, compiled.where, compiled.projector));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoFilterForHashJoinScan() throws Exception {
    Compiled compiled = compile("I = 5");
    Scan join = new Scan(compiled.scan);
    Set<String> before = new HashSet<>(join.getAttributesMap().keySet());
    HashJoinInfo.serializeHashJoinIntoScan(join, new HashJoinInfo(null, new ImmutableBytesPtr[0],
      new List[0], new JoinType[0], new boolean[0], new PTable[0], new int[0], null, null));
    assertTrue(HashJoinInfo.hasHashJoin(join));
    assertNull(UncoveredIndexDataRowFilter.create(join, compiled.where, compiled.projector));
    // The check only looks for the join attribute. It does not read the join, so bytes that do
    // not decode do not make the check fail.
    Set<String> added = new HashSet<>(join.getAttributesMap().keySet());
    added.removeAll(before);
    assertEquals(1, added.size());
    join.setAttribute(added.iterator().next(), new byte[] { (byte) 0xFF });
    assertNull(UncoveredIndexDataRowFilter.create(join, compiled.where, compiled.projector));
    assertFalse(HashJoinInfo.hasHashJoin(compiled.scan));
  }

  /**
   * Checks that each conjunct and each column read give the same value on the data row as the
   * original gives on the joined row. It also checks the filter decision against the full WHERE.
   * @param expected the expected value of the full WHERE, or null to skip that check
   */
  private void assertEquivalent(Compiled compiled, Map<String, Object> values, Boolean expected)
    throws IOException, SQLException {
    Result dataRow = dataRow(compiled.dataTable, values);
    Tuple joined = joinedTuple(compiled, dataRow);
    Tuple data = new ResultTuple(dataRow);
    for (Expression conjunct : UncoveredIndexDataRowFilter.getConjuncts(compiled.where)) {
      Expression rewritten =
        UncoveredIndexDataRowFilter.rewriteConjunct(conjunct, compiled.projector);
      assertNotNull(conjunct.toString(), rewritten);
      assertSameValue(conjunct, joined, rewritten, data);
    }
    for (ProjectedColumnExpression column : findProjectedColumns(compiled.where)) {
      assertSameValue(column, joined,
        UncoveredIndexDataRowFilter.rewrite(column, compiled.projector), data);
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    compiled.where.reset();
    boolean complete = compiled.where.evaluate(joined, ptr);
    boolean accepted = complete && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
    String message = compiled.where + " on " + values;
    if (expected != null) {
      assertTrue(message, complete);
      assertEquals(message, expected, accepted);
    }
    UncoveredIndexDataRowFilter filter =
      UncoveredIndexDataRowFilter.create(compiled.scan, compiled.where, compiled.projector);
    assertNotNull(filter);
    // An incomplete evaluation keeps the row, and the post-join WHERE decides.
    assertEquals(message, complete && !accepted, filter.rejects(dataRow));
  }

  private static void assertSameValue(Expression original, Tuple joined, Expression rewritten,
    Tuple data) {
    ImmutableBytesWritable expected = new ImmutableBytesWritable();
    ImmutableBytesWritable actual = new ImmutableBytesWritable();
    original.reset();
    rewritten.reset();
    boolean expectedComplete = original.evaluate(joined, expected);
    boolean actualComplete = rewritten.evaluate(data, actual);
    assertEquals(original.toString(), expectedComplete, actualComplete);
    if (expectedComplete) {
      assertArrayEquals(original.toString(), ByteUtil.copyKeyBytesIfNecessary(expected),
        ByteUtil.copyKeyBytesIfNecessary(actual));
    }
  }

  private Compiled compile(String where) throws SQLException, IOException {
    return compile(table, index, "K", where);
  }

  private Compiled compile(String tableName, String indexName, String select, String where)
    throws SQLException, IOException {
    try (PhoenixConnection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES))
        .unwrap(PhoenixConnection.class)) {
      String sql = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ " + select + " FROM "
        + tableName + " WHERE " + where;
      QueryPlan plan = conn.createStatement().unwrap(PhoenixStatement.class).optimizeQuery(sql);
      assertEquals(indexName, plan.getTableRef().getTable().getTableName().getString());
      StatementContext context = plan.getContext();
      Scan scan = new Scan(context.getScan());
      byte[] filterBytes = scan.getAttribute(BaseScannerRegionObserverConstants.INDEX_FILTER);
      assertNotNull(sql, filterBytes);
      PTable dataTable = conn.getTable(tableName);
      // Set the join attributes in the same way as the client does before it runs the scan.
      Set<PColumn> dataColumns = context.getDataColumns();
      KeyValueSchema schema = ProjectedColumnExpression.buildSchema(dataColumns);
      ByteArrayOutputStream schemaBytes = new ByteArrayOutputStream();
      try (DataOutputStream output = new DataOutputStream(schemaBytes)) {
        schema.write(output);
      }
      scan.setAttribute(BaseScannerRegionObserverConstants.LOCAL_INDEX_JOIN_SCHEMA,
        schemaBytes.toByteArray());
      if (
        dataTable.getImmutableStorageScheme()
            == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
      ) {
        scan.setAttribute(BaseScannerRegionObserverConstants.COLUMNS_STORED_IN_SINGLE_CELL,
          QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        scan.setAttribute(BaseScannerRegionObserverConstants.QUALIFIER_ENCODING_SCHEME,
          new byte[] { dataTable.getEncodingScheme().getSerializedMetadataValue() });
        scan.setAttribute(BaseScannerRegionObserverConstants.IMMUTABLE_STORAGE_ENCODING_SCHEME,
          new byte[] { dataTable.getImmutableStorageScheme().getSerializedMetadataValue() });
      }
      List<ColumnReference> references = new ArrayList<>();
      for (PColumn column : dataColumns) {
        references.add(
          new ColumnReference(column.getFamilyName().getBytes(), column.getColumnQualifierBytes()));
      }
      TupleProjector projector =
        IndexUtil.getTupleProjector(scan, references.toArray(new ColumnReference[0]));
      return new Compiled(scan, deserialize(filterBytes), projector, dataTable);
    }
  }

  /** Reads the filter in the same way as the region server does. */
  private static Expression deserialize(byte[] bytes) throws IOException {
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
    Expression expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
    expression.readFields(input);
    return expression;
  }

  private static Result dataRow(PTable dataTable, Map<String, Object> values) throws SQLException {
    List<Cell> cells = new ArrayList<>();
    cells.add(new KeyValue(DATA_ROW, SchemaUtil.getEmptyColumnFamily(dataTable),
      EncodedColumnsUtil.getEmptyKeyValueInfo(dataTable).getFirst(), TS,
      QueryConstants.EMPTY_COLUMN_VALUE_BYTES));
    if (
      dataTable.getImmutableStorageScheme() != ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
    ) {
      for (Map.Entry<String, Object> entry : values.entrySet()) {
        PColumn column = dataTable.getColumnForColumnName(entry.getKey());
        cells.add(new KeyValue(DATA_ROW, column.getFamilyName().getBytes(),
          column.getColumnQualifierBytes(), TS, column.getDataType().toBytes(entry.getValue())));
      }
    } else {
      assertTrue(values.isEmpty());
    }
    cells.sort(CellComparator.getInstance());
    return Result.create(cells);
  }

  /** Builds the joined row in the same way as the uncovered index scanner does. */
  private static Tuple joinedTuple(Compiled compiled, Result dataRow) {
    List<Cell> cells = new ArrayList<>();
    cells.add(new KeyValue(INDEX_ROW, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
      QueryConstants.EMPTY_COLUMN_BYTES, TS, QueryConstants.VERIFIED_BYTES));
    IndexUtil.addTupleAsOneCell(cells, new ResultTuple(dataRow), compiled.projector,
      new ImmutableBytesWritable());
    cells.sort(CellComparator.getInstance());
    return new ResultTuple(Result.create(cells));
  }

  private static List<ProjectedColumnExpression> findProjectedColumns(Expression expression) {
    List<ProjectedColumnExpression> columns = new ArrayList<>();
    addProjectedColumns(expression, columns);
    return columns;
  }

  private static void addProjectedColumns(Expression expression,
    List<ProjectedColumnExpression> columns) {
    if (expression instanceof ProjectedColumnExpression) {
      columns.add((ProjectedColumnExpression) expression);
    }
    for (Expression child : expression.getChildren()) {
      addProjectedColumns(child, columns);
    }
  }

  private static PDatum datum(PDataType type, SortOrder sortOrder) {
    return new PDatum() {
      @Override
      public boolean isNullable() {
        return true;
      }

      @Override
      public PDataType getDataType() {
        return type;
      }

      @Override
      public Integer getMaxLength() {
        return null;
      }

      @Override
      public Integer getScale() {
        return null;
      }

      @Override
      public SortOrder getSortOrder() {
        return sortOrder;
      }
    };
  }

  /** A boolean test expression. */
  private abstract static class TestBoolean extends BaseTerminalExpression {
    @Override
    public PDataType getDataType() {
      return PBoolean.INSTANCE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
      return visitor.defaultReturn(this, Collections.emptyList());
    }
  }
}
