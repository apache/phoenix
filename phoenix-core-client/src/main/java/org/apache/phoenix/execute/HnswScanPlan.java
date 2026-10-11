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
package org.apache.phoenix.execute;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.function.BsonVectorValueFunction;
import org.apache.phoenix.expression.function.DistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceSquaredFunction;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ProjectedColumn;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.CostUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Optional;

/**
 * Query plan for a nearest neighbor search with an HNSW index. The plan scans the data table. Each
 * region limits its scan to the candidate rows of a search of its own graph. The region servers
 * rank these rows by exact distance. If the WHERE clause bounds the distance of the ORDER BY, the
 * plan also sends the bound. A region then stops its search when the graph gives no more rows
 * within the bound.
 * <p>
 * A filter on a column that is not in the row key can remove most candidates. A region then asks
 * its graph for more candidates. If too few rows of a scan range pass the filter, the region reads
 * every row of that range. For a filter that is known to pass very few rows, the
 * USE_DATA_OVER_INDEX_TABLE hint ranks this plan after the other plans, and the optimizer can still
 * use a secondary index on the filter column. The NO_INDEX hint always gives an exact scan of the
 * data table without the graph search, but it also disables all secondary indexes.
 */
public class HnswScanPlan extends ScanPlan {
  private final PTable index;
  private final int candidates;
  private final double radius;

  public HnswScanPlan(StatementContext context, FilterableStatement statement, TableRef dataTable,
    RowProjector projector, Integer limit, Integer offset, OrderBy orderBy,
    ParallelIteratorFactory parallelIteratorFactory, boolean allowPageFilter,
    Optional<byte[]> rowOffset, PTable index, Expression where) throws SQLException {
    super(context, statement, dataTable, projector, limit, offset, orderBy, parallelIteratorFactory,
      allowPageFilter, null, rowOffset);
    this.index = index;
    int efSearch = VectorIndexScanPlan.getHintInt(statement.getHint(), Hint.HNSW_EF_SEARCH,
      context.getConnection().getQueryServices().getProps()
        .getInt(QueryServices.HNSW_EF_SEARCH_ATTRIB, QueryServicesOptions.DEFAULT_HNSW_EF_SEARCH));
    int target = limit + (offset != null ? offset : 0);
    this.candidates = Math.max(efSearch, target);
    float[] query = queryVector(orderBy);
    this.radius = radius(where, orderBy, query);
    // The search attribute holds the candidate count, the target row count and the query vector.
    // The client merges the rows of all regions, so each region tries to return the target count.
    // A distance bound follows the vector, if the WHERE clause gives one
    int length = Bytes.SIZEOF_INT * (2 + query.length);
    byte[] search = new byte[Double.isNaN(radius) ? length : length + Bytes.SIZEOF_FLOAT];
    Bytes.putInt(search, 0, candidates);
    Bytes.putInt(search, Bytes.SIZEOF_INT, target);
    for (int i = 0; i < query.length; i++) {
      Bytes.putFloat(search, Bytes.SIZEOF_INT * (2 + i), query[i]);
    }
    if (!Double.isNaN(radius)) {
      Bytes.putFloat(search, length, (float) radius);
    }
    // A view inherits the index as <view>#<index>. The regions keep one graph under the index name
    context.getScan().setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_INDEX,
      Bytes.toBytes(CentroidManager.getCentroidIndexName(index.getName().getString())));
    context.getScan().setAttribute(BaseScannerRegionObserverConstants.HNSW_SEARCH_QUERY, search);
  }

  /** Returns the constant query vector of the ORDER BY distance function, or null if none. */
  public static float[] queryVector(OrderBy orderBy) {
    return VectorIndexScanPlan.getQueryVector(orderBy);
  }

  /**
   * Returns the smallest upper bound of the ORDER BY distance that a conjunct of the WHERE clause
   * gives, or NaN if there is none. The conjuncts include the terms of nested AND expressions, for
   * example from BETWEEN or from the WHERE clause of a view. A conjunct gives a bound if it
   * compares the same distance function of the same column and query vector with a deterministic
   * constant. The bound has the units of the index metric. Thus the bound of a squared L2 distance
   * is its square root.
   */
  static double radius(Expression where, OrderBy orderBy, float[] query) {
    if (where == null || query == null) {
      return Double.NaN;
    }
    Expression distance = orderBy.getOrderByExpressions().get(0).getExpression();
    List<Expression> conjuncts = new ArrayList<>();
    conjuncts(where, conjuncts);
    double radius = Double.NaN;
    for (Expression conjunct : conjuncts) {
      double bound = bound(conjunct, distance, query);
      if (!Double.isNaN(bound) && !(bound >= radius)) {
        radius = bound;
      }
    }
    if (Double.isNaN(radius)) {
      return radius;
    }
    radius = Math.max(0.0, radius);
    return distance instanceof L2DistanceSquaredFunction ? Math.sqrt(radius) : radius;
  }

  // Adds the terms of an AND tree to the list. Each term must be true for a row to pass
  private static void conjuncts(Expression expression, List<Expression> conjuncts) {
    if (expression instanceof AndExpression) {
      for (Expression child : expression.getChildren()) {
        conjuncts(child, conjuncts);
      }
    } else {
      conjuncts.add(expression);
    }
  }

  // Returns the upper bound of the distance that a comparison gives, or NaN if it gives none. The
  // statement normalizer moves a constant operand to the right side, so a bound has the distance
  // on the left side
  private static double bound(Expression conjunct, Expression distance, float[] query) {
    if (!(conjunct instanceof ComparisonExpression)) {
      return Double.NaN;
    }
    CompareOperator op = ((ComparisonExpression) conjunct).getFilterOp();
    boolean below = op == CompareOperator.LESS || op == CompareOperator.LESS_OR_EQUAL;
    return below && sameDistance(conjunct.getChildren().get(0), distance, query)
      ? constant(conjunct.getChildren().get(1))
      : Double.NaN;
  }

  // Returns true if the expression is the distance function of the ORDER BY, with the same column
  // and the same query vector in the same places
  private static boolean sameDistance(Expression expression, Expression distance, float[] query) {
    if (!(expression instanceof DistanceFunction) || expression.getClass() != distance.getClass()) {
      return false;
    }
    List<Expression> children = expression.getChildren();
    List<Expression> expected = distance.getChildren();
    if (children.size() != expected.size()) {
      return false;
    }
    for (int i = 0; i < children.size(); i++) {
      float[] vector = VectorIndexScanPlan.constantVector(children.get(i));
      boolean same = vector != null
        ? VectorIndexScanPlan.constantVector(expected.get(i)) != null
          && Arrays.equals(vector, query) && sameConstant(children.get(i), expected.get(i))
        : sameColumn(children.get(i), expected.get(i));
      if (!same) {
        return false;
      }
    }
    return true;
  }

  // Returns true if two constants have the same type, sort order and bytes. The search uses a float
  // copy of the query vector. A constant of a wider type can have the same float copy, but the
  // distance function then uses the wider value
  private static boolean sameConstant(Expression constant, Expression expected) {
    if (
      constant.getDataType() != expected.getDataType()
        || constant.getSortOrder() != expected.getSortOrder()
    ) {
      return false;
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    ImmutableBytesWritable other = new ImmutableBytesWritable();
    return constant.evaluate(null, ptr) && expected.evaluate(null, other) && Bytes.equals(ptr.get(),
      ptr.getOffset(), ptr.getLength(), other.get(), other.getOffset(), other.getLength());
  }

  // Returns true only if the two expressions always compute the same vector. The WHERE clause reads
  // raw cells, but the ORDER BY reads the projected row. Thus one column can have a different
  // expression type in each clause. Two columns match if they have the same column family and
  // column qualifier. Only a small set of expression shapes can match. Each other shape gives no
  // bound. A missed bound makes the search slower, but a wrong bound removes rows from the result
  private static boolean sameColumn(Expression expression, Expression expected) {
    if (sameLeaf(expression, expected)) {
      return true;
    }
    // An index can be on a vector in a BSON document. The function gets its path and dimension from
    // its children. Thus two such functions with matching children compute the same vector
    if (
      expression.getClass() != BsonVectorValueFunction.class
        || expected.getClass() != BsonVectorValueFunction.class
    ) {
      return false;
    }
    List<Expression> children = expression.getChildren();
    List<Expression> arguments = expected.getChildren();
    if (children.size() != arguments.size()) {
      return false;
    }
    for (int i = 0; i < children.size(); i++) {
      if (!sameLeaf(children.get(i), arguments.get(i))) {
        return false;
      }
    }
    return true;
  }

  // Returns true if both expressions read the same column of a column family. Two literals match
  // only if they have the same type and an equal string or integer value. A function argument needs
  // no other value type. The equals method of a literal does not compare the type. Also, the equals
  // method of some value types accepts other value types, for example a date and a timestamp
  private static boolean sameLeaf(Expression expression, Expression expected) {
    byte[][] column = column(expression);
    byte[][] other = column(expected);
    if (column != null || other != null) {
      return column != null && other != null && Bytes.equals(column[0], other[0])
        && Bytes.equals(column[1], other[1]);
    }
    if (!(expression instanceof LiteralExpression) || !(expected instanceof LiteralExpression)) {
      return false;
    }
    Object value = ((LiteralExpression) expression).getValue();
    return (value instanceof String || value instanceof Integer)
      && expression.getDataType() == expected.getDataType() && expression.equals(expected);
  }

  // Returns the column family and the column qualifier that a column expression reads, or null if
  // the expression is not a column of a column family
  private static byte[][] column(Expression expression) {
    if (expression instanceof SingleCellColumnExpression) {
      SingleCellColumnExpression cell = (SingleCellColumnExpression) expression;
      return new byte[][] { cell.getColumnFamily(), cell.getPositionInArray() };
    }
    if (expression instanceof KeyValueColumnExpression) {
      KeyValueColumnExpression cell = (KeyValueColumnExpression) expression;
      return new byte[][] { cell.getColumnFamily(), cell.getColumnQualifier() };
    }
    if (!(expression instanceof ProjectedColumnExpression)) {
      return null;
    }
    PColumn column = ((ProjectedColumnExpression) expression).getColumn();
    if (column instanceof ProjectedColumn) {
      column = ((ProjectedColumn) column).getSourceColumnRef().getColumn();
    }
    if (column.getFamilyName() == null) {
      return null;
    }
    byte[] qualifier = column.getColumnQualifierBytes() != null
      ? column.getColumnQualifierBytes()
      : column.getName().getBytes();
    return new byte[][] { column.getFamilyName().getBytes(), qualifier };
  }

  // Returns the value of a deterministic constant numeric expression, or NaN if the expression is
  // not one. A function such as RAND() is stateless but gives a new value for each evaluation
  private static double constant(Expression expression) {
    if (
      !expression.isStateless() || expression.getDeterminism() != Determinism.ALWAYS
        || expression.getDataType() == null
    ) {
      return Double.NaN;
    }
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    if (!expression.evaluate(null, ptr) || ptr.getLength() == 0) {
      return Double.NaN;
    }
    Object value = expression.getDataType().toObject(ptr, expression.getSortOrder());
    return value instanceof Number ? ((Number) value).doubleValue() : Double.NaN;
  }

  /** Returns the HNSW index that the search uses. */
  public PTable getIndex() {
    return index;
  }

  /**
   * Estimates the cost as point reads of the candidate rows of each scan, because each region scan
   * searches its own graph. The estimate is at most the rows of the scanned range. Returns
   * {@link Cost#UNKNOWN} if the statistics give no row estimate. The estimate does not include the
   * rows that a scan adds when its candidates fail the filter. Such a scan can read every row of
   * its range. A region without a covering base segment also reads every row of its range.
   */
  @Override
  public Cost getCost() {
    Long rows;
    try {
      rows = getEstimatedRowsToScan();
    } catch (SQLException e) {
      return Cost.UNKNOWN;
    }
    List<List<Scan>> scans = getScans();
    if (rows == null || scans == null) {
      return Cost.UNKNOWN;
    }
    long fanOut = 0;
    for (List<Scan> group : scans) {
      fanOut += group.size();
    }
    return CostUtil.estimateLookupCost(Math.min(rows, fanOut * candidates),
      getTableRef().getTable());
  }

  @Override
  public ExplainPlan getExplainPlan() throws SQLException {
    ExplainPlan plan = super.getExplainPlan();
    List<String> steps = new ArrayList<>(plan.getPlanSteps());
    steps.add(1, "    SERVER HNSW SEARCH " + index.getName().getString() + " (" + candidates
      + " CANDIDATES" + (Double.isNaN(radius) ? "" : ", DISTANCE BOUND " + (float) radius) + ")");
    return new ExplainPlan(steps, plan.getPlanStepsAsAttributes());
  }
}
