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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ArithmeticExpression;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.function.BsonValueFunction;
import org.apache.phoenix.expression.function.CoalesceFunction;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueSchema.Field;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

/**
 * An early filter for uncovered index scans. The scanner applies it to each data row before it
 * joins the data row to the index row. The filter holds the leading conjuncts of the post-join
 * WHERE that read only data columns. It reads those columns from the data row directly. A rejected
 * row then skips the join. The full post-join WHERE still runs on each accepted row, so the filter
 * only removes rows that the post-join WHERE also removes. The filter also does not hide an error
 * that the post-join WHERE raises on a row.
 */
public final class UncoveredIndexDataRowFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(UncoveredIndexDataRowFilter.class);

  /**
   * The bit set of a value schema has a different format above this count of nullable fields.
   */
  private static final int MAX_SHORT_BIT_SET_FIELDS = 16;

  private final Expression expression;
  private final boolean complete;
  private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

  @VisibleForTesting
  UncoveredIndexDataRowFilter(Expression expression) {
    this(expression, true);
  }

  /**
   * @param expression the conjuncts that the filter applies to the data row
   * @param complete   true when the expression holds all conjuncts of the post-join WHERE
   */
  private UncoveredIndexDataRowFilter(Expression expression, boolean complete) {
    this.expression = expression;
    this.complete = complete;
  }

  /**
   * Builds the early filter from the post-join WHERE of an uncovered index scan.
   * @param scan           the index scan
   * @param extraWhere     the post-join WHERE, or null
   * @param tupleProjector the projector that builds the joined row from the data row, or null
   * @return the filter, or null when no conjunct is safe to apply early
   */
  public static UncoveredIndexDataRowFilter create(Scan scan, Expression extraWhere,
    TupleProjector tupleProjector) {
    // Analyze scans skip the post-join WHERE. A hash join reads the joined row in a different
    // layout. In these cases the filter must not change the rows.
    if (
      extraWhere == null || tupleProjector == null || ScanUtil.isAnalyzeTable(scan)
        || HashJoinInfo.hasHashJoin(scan)
    ) {
      return null;
    }
    // The post-join AND evaluates its conjuncts in order and stops at the first FALSE. The filter
    // takes only the leading conjuncts that it can rewrite. A FALSE in this prefix then means that
    // the post-join WHERE stops before it evaluates a conjunct that the filter does not hold.
    List<Expression> conjuncts = getConjuncts(extraWhere);
    List<Expression> pushed = new ArrayList<>();
    for (Expression conjunct : conjuncts) {
      Expression rewritten = rewriteConjunct(conjunct, tupleProjector);
      if (rewritten == null) {
        break;
      }
      pushed.add(rewritten);
    }
    if (pushed.isEmpty()) {
      return null;
    }
    return new UncoveredIndexDataRowFilter(
      pushed.size() == 1 ? pushed.get(0) : new AndExpression(pushed),
      pushed.size() == conjuncts.size());
  }

  /**
   * Tells if the data row fails the filter. The filter rejects a row only when the evaluation
   * completes and the result is FALSE. When the filter holds all conjuncts of the WHERE, a NULL
   * result also rejects the row. Otherwise a NULL result keeps the row, because the post-join AND
   * continues after a NULL and a later conjunct can raise an error. An incomplete evaluation or a
   * runtime error keeps the row. The post-join WHERE then decides, and it reports any error in the
   * usual order.
   * @param dataRow the data row that the scanner joins to the index row
   * @return true if the scanner can skip the row
   */
  public boolean rejects(Result dataRow) {
    try {
      // Some expressions keep state from the last row. Clear it before each row.
      expression.reset();
      if (!expression.evaluate(new ResultTuple(dataRow), ptr)) {
        return false;
      }
      Object result = expression.getDataType().toObject(ptr);
      return complete ? !Boolean.TRUE.equals(result) : Boolean.FALSE.equals(result);
    } catch (RuntimeException e) {
      LOGGER.debug("Early filter failed on a data row, keeping the row", e);
      return false;
    }
  }

  @VisibleForTesting
  Expression getExpression() {
    return expression;
  }

  /**
   * Splits nested AND expressions into a flat list of conjuncts.
   */
  @VisibleForTesting
  static List<Expression> getConjuncts(Expression where) {
    List<Expression> conjuncts = new ArrayList<>();
    addConjuncts(where, conjuncts);
    return conjuncts;
  }

  private static void addConjuncts(Expression expression, List<Expression> conjuncts) {
    if (expression.getClass() == AndExpression.class) {
      for (Expression child : expression.getChildren()) {
        addConjuncts(child, conjuncts);
      }
    } else {
      conjuncts.add(expression);
    }
  }

  /**
   * Rewrites one conjunct so that it reads the data row instead of the joined row.
   * @return the rewritten conjunct, or null when the conjunct is not safe to apply early
   */
  @VisibleForTesting
  static Expression rewriteConjunct(Expression conjunct, TupleProjector tupleProjector) {
    if (conjunct.getDataType() != PBoolean.INSTANCE) {
      return null;
    }
    try {
      Expression rewritten = rewrite(conjunct, tupleProjector);
      if (rewritten == null || containsProjectedColumn(rewritten)) {
        return null;
      }
      return rewritten;
    } catch (IOException | RuntimeException e) {
      LOGGER.debug("Cannot apply conjunct {} early", conjunct, e);
      return null;
    }
  }

  /**
   * Copies the expression tree and replaces each projected column with a read of the data row. Only
   * a fixed set of node types is allowed. These nodes read the tuple only through their children.
   * Other nodes, for example index columns or functions that read the row key, give a different
   * answer on the data row. Each node must give the same result for each evaluation on the same
   * row.
   * @return the copy, or null when a node is not allowed
   */
  @VisibleForTesting
  static Expression rewrite(Expression node, TupleProjector tupleProjector) throws IOException {
    Determinism determinism = node.getDeterminism();
    if (determinism != Determinism.ALWAYS && determinism != Determinism.PER_STATEMENT) {
      return null;
    }
    if (node instanceof ProjectedColumnExpression) {
      return readDataColumn((ProjectedColumnExpression) node, tupleProjector);
    }
    if (node instanceof LiteralExpression) {
      return node;
    }
    List<Expression> children = new ArrayList<>(node.getChildren().size());
    for (Expression child : node.getChildren()) {
      Expression rewrittenChild = rewrite(child, tupleProjector);
      if (rewrittenChild == null) {
        return null;
      }
      children.add(rewrittenChild);
    }
    Class<? extends Expression> type = node.getClass();
    if (type == AndExpression.class) {
      return new AndExpression(children);
    } else if (type == OrExpression.class) {
      return new OrExpression(children);
    } else if (type == NotExpression.class) {
      return new NotExpression(children);
    } else if (type == CaseExpression.class) {
      return new CaseExpression(children);
    } else if (type == ComparisonExpression.class) {
      return ((ComparisonExpression) node).clone(children);
    } else if (type == IsNullExpression.class) {
      return ((IsNullExpression) node).clone(children);
    } else if (type == CoerceExpression.class) {
      return ((CoerceExpression) node).clone(children);
    } else if (type == InListExpression.class) {
      return new InListCopy((InListExpression) node, children.get(0));
    } else if (node instanceof LikeExpression) {
      return ((LikeExpression) node).clone(children);
    } else if (node instanceof ArithmeticExpression) {
      return ((ArithmeticExpression) node).clone(children);
    } else if (type == CoalesceFunction.class || type == BsonValueFunction.class) {
      return ((ScalarFunction) node).clone(children);
    }
    return null;
  }

  /**
   * Replaces a projected column with a read of the same column from the data row. The joined row
   * stores the value of each projector expression at its position. The replacement reads the value
   * at the same position. The replacement is valid only when the projected column decodes that
   * position in the same way as the joined row encodes it.
   */
  private static Expression readDataColumn(ProjectedColumnExpression column,
    TupleProjector tupleProjector) {
    KeyValueSchema columnSchema = column.getSchema();
    KeyValueSchema joinSchema = tupleProjector.getSchema();
    Expression[] sources = tupleProjector.getExpressions();
    int position = column.getPosition();
    if (
      position < 0 || position >= columnSchema.getFieldCount() || position >= sources.length
        || !isPrefix(columnSchema, joinSchema)
    ) {
      return null;
    }
    Expression source = sources[position];
    if (
      source.getDataType() != column.getDataType() || source.getSortOrder() != column.getSortOrder()
    ) {
      return null;
    }
    return new DataColumnExpression(column, source);
  }

  /**
   * Tells if the first schema decodes its fields from bytes of the second schema without change.
   * Each field must have the same type, sort order and size at the same position. The schemas must
   * also agree on the nullable fields and on the format of the null bit set.
   */
  @VisibleForTesting
  static boolean isPrefix(KeyValueSchema prefix, KeyValueSchema schema) {
    int prefixCount = prefix.getFieldCount();
    int count = schema.getFieldCount();
    if (prefixCount > count || prefix.getMinNullable() != schema.getMinNullable()) {
      return false;
    }
    // A schema without nullable fields has no bit set. A schema with more nullable fields than
    // the short limit uses a longer bit set.
    int prefixNullable = prefixCount - prefix.getMinNullable();
    int nullable = count - schema.getMinNullable();
    if (
      (prefixNullable == 0) != (nullable == 0)
        || (prefixNullable > MAX_SHORT_BIT_SET_FIELDS) != (nullable > MAX_SHORT_BIT_SET_FIELDS)
    ) {
      return false;
    }
    for (int i = 0; i < prefixCount; i++) {
      Field a = prefix.getField(i);
      Field b = schema.getField(i);
      if (
        a.getDataType() != b.getDataType() || a.getSortOrder() != b.getSortOrder()
          || (a.getDataType().isFixedWidth() && a.getByteSize() != b.getByteSize())
      ) {
        return false;
      }
    }
    return true;
  }

  /**
   * Tells if a projected column is left in the tree.
   */
  @VisibleForTesting
  static boolean containsProjectedColumn(Expression expression) {
    if (expression instanceof ProjectedColumnExpression) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (containsProjectedColumn(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reads one data column from the data row with the semantics of a projected column. A missing
   * value is NULL. The expression lives only on the server and is never serialized.
   */
  @VisibleForTesting
  static final class DataColumnExpression extends BaseTerminalExpression {
    private final ProjectedColumnExpression column;
    private final Expression source;

    DataColumnExpression(ProjectedColumnExpression column, Expression source) {
      this.column = column;
      this.source = source;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
      // The joined row stores a value only when the source gives a non-empty value. In all
      // other cases the projected column reads NULL. Do the same here.
      if (!source.evaluate(tuple, ptr) || ptr.getLength() == 0) {
        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
      }
      return true;
    }

    @Override
    public PDataType getDataType() {
      return column.getDataType();
    }

    @Override
    public SortOrder getSortOrder() {
      return column.getSortOrder();
    }

    @Override
    public Integer getMaxLength() {
      return column.getMaxLength();
    }

    @Override
    public Integer getScale() {
      return column.getScale();
    }

    @Override
    public boolean isNullable() {
      return column.isNullable();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
      return visitor.defaultReturn(this, Collections.emptyList());
    }

    @Override
    public void write(DataOutput output) throws IOException {
      throw new UnsupportedOperationException("A data column read is never serialized");
    }

    @Override
    public String toString() {
      return column.toString();
    }
  }

  /**
   * A copy of an IN expression with a new child. The values of an IN expression are not children,
   * and the server copy has no key expressions to build them from. So the copy takes the values
   * from the serialized form of the original.
   */
  private static final class InListCopy extends InListExpression {
    InListCopy(InListExpression original, Expression child) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream output = new DataOutputStream(bytes)) {
        original.write(output);
      }
      try (DataInputStream input =
        new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        readFields(input);
      }
      this.children = ImmutableList.of(child);
    }

    @Override
    public void write(DataOutput output) throws IOException {
      throw new UnsupportedOperationException("An early filter copy is never serialized");
    }
  }
}
