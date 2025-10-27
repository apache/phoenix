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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.CloneExpressionVisitor;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Class that manages a set of projected columns accessed through the zero-based column index for a
 * SELECT clause projection. The column index may be looked up via the name using
 * {@link #getColumnIndex(String)}.
 * @since 0.1
 */
public class RowProjector {
  public static final RowProjector EMPTY_PROJECTOR =
    new RowProjector(Collections.<ColumnProjector> emptyList(), 0, true);

  private final List<? extends ColumnProjector> columnProjectors;
  private final ListMultimap<String, Integer> reverseIndex;
  private final boolean allCaseSensitive;
  private final boolean someCaseSensitive;
  private final int estimatedSize;
  private final boolean isProjectAll;
  private final boolean isProjectEmptyKeyValue;
  private final boolean cloneRequired;
  private final boolean hasUDFs;
  private final boolean isProjectDynColsInWildcardQueries;

  public RowProjector(RowProjector projector, boolean isProjectEmptyKeyValue) {
    this(projector.getColumnProjectors(), projector.getEstimatedRowByteSize(),
      isProjectEmptyKeyValue, projector.hasUDFs, projector.isProjectAll,
      projector.isProjectDynColsInWildcardQueries);
  }

  /**
   * Construct RowProjector based on a list of ColumnProjectors.
   * @param columnProjectors ordered list of ColumnProjectors corresponding to projected columns in
   *                         SELECT clause aggregating coprocessor. Only required in the case of an
   *                         aggregate query with a limit clause and otherwise may be null.
   */
  public RowProjector(List<? extends ColumnProjector> columnProjectors, int estimatedRowSize,
    boolean isProjectEmptyKeyValue) {
    this(columnProjectors, estimatedRowSize, isProjectEmptyKeyValue, false, false, false);
  }

  /**
   * Construct RowProjector based on a list of ColumnProjectors.
   * @param columnProjectors ordered list of ColumnProjectors corresponding to projected columns in
   *                         SELECT clause aggregating coprocessor. Only required in the case of an
   *                         aggregate query with a limit clause and otherwise may be null.
   */
  public RowProjector(List<? extends ColumnProjector> columnProjectors, int estimatedRowSize,
    boolean isProjectEmptyKeyValue, boolean hasUDFs, boolean isProjectAll,
    boolean isProjectDynColsInWildcardQueries) {
    this(columnProjectors, estimatedRowSize, isProjectEmptyKeyValue, hasUDFs, isProjectAll,
      isProjectDynColsInWildcardQueries, null);
  }

  /**
   * Construct RowProjector based on a list of ColumnProjectors with additional name mappings.
   * @param columnProjectors       ordered list of ColumnProjectors corresponding to projected
   *                               columns in SELECT clause aggregating coprocessor. Only required
   *                               in the case of an aggregate query with a limit clause and
   *                               otherwise may be null.
   * @param additionalNameMappings additional column name to position mappings to merge into the
   *                               reverseIndex during construction. Used for preserving original
   *                               column names when query optimization rewrites them (e.g., view
   *                               constants). May be null.
   */
  public RowProjector(List<? extends ColumnProjector> columnProjectors, int estimatedRowSize,
    boolean isProjectEmptyKeyValue, boolean hasUDFs, boolean isProjectAll,
    boolean isProjectDynColsInWildcardQueries,
    ListMultimap<String, Integer> additionalNameMappings) {
    this.columnProjectors = Collections.unmodifiableList(columnProjectors);
    int position = columnProjectors.size();
    reverseIndex = ArrayListMultimap.<String, Integer> create();
    boolean allCaseSensitive = true;
    boolean someCaseSensitive = false;
    for (--position; position >= 0; position--) {
      ColumnProjector colProjector = columnProjectors.get(position);
      allCaseSensitive &= colProjector.isCaseSensitive();
      someCaseSensitive |= colProjector.isCaseSensitive();
      reverseIndex.put(colProjector.getLabel(), position);
      if (!colProjector.getTableName().isEmpty()) {
        reverseIndex.put(
          SchemaUtil.getColumnName(colProjector.getTableName(), colProjector.getLabel()), position);
      }
    }

    // Merge additional name mappings if provided (for PHOENIX-6644)
    if (additionalNameMappings != null) {
      for (String name : additionalNameMappings.keySet()) {
        if (!reverseIndex.containsKey(name)) {
          reverseIndex.putAll(name, additionalNameMappings.get(name));
        }
      }
    }

    this.allCaseSensitive = allCaseSensitive;
    this.someCaseSensitive = someCaseSensitive;
    this.estimatedSize = estimatedRowSize;
    this.isProjectEmptyKeyValue = isProjectEmptyKeyValue;
    this.isProjectAll = isProjectAll;
    this.hasUDFs = hasUDFs;
    boolean cloneRequired = false;
    if (!hasUDFs) {
      for (int i = 0; i < this.columnProjectors.size(); i++) {
        Expression expression = this.columnProjectors.get(i).getExpression();
        if (expression.isCloneExpression()) {
          cloneRequired = true;
          break;
        }
      }
    }
    this.cloneRequired = cloneRequired || hasUDFs;
    this.isProjectDynColsInWildcardQueries = isProjectDynColsInWildcardQueries;
  }

  public RowProjector cloneIfNecessary() {
    if (!cloneRequired) {
      return this;
    }
    List<ColumnProjector> clonedColProjectors =
      new ArrayList<ColumnProjector>(columnProjectors.size());
    for (int i = 0; i < this.columnProjectors.size(); i++) {
      ColumnProjector colProjector = columnProjectors.get(i);
      Expression expression = colProjector.getExpression();
      if (expression.isCloneExpression()) {
        CloneExpressionVisitor visitor = new CloneExpressionVisitor();
        Expression clonedExpression = expression.accept(visitor);
        clonedColProjectors
          .add(new ExpressionProjector(colProjector.getName(), colProjector.getLabel(),
            colProjector.getTableName(), clonedExpression, colProjector.isCaseSensitive()));
      } else {
        clonedColProjectors.add(colProjector);
      }
    }
    return new RowProjector(clonedColProjectors, this.estimatedSize, this.isProjectEmptyKeyValue,
      this.hasUDFs, this.isProjectAll, this.isProjectDynColsInWildcardQueries);
  }

  public boolean projectEveryRow() {
    return isProjectEmptyKeyValue;
  }

  public boolean projectEverything() {
    return isProjectAll;
  }

  public boolean hasUDFs() {
    return hasUDFs;
  }

  public boolean projectDynColsInWildcardQueries() {
    return isProjectDynColsInWildcardQueries;
  }

  public List<? extends ColumnProjector> getColumnProjectors() {
    return columnProjectors;
  }

  public int getColumnIndex(String name) throws SQLException {
    if (!someCaseSensitive) {
      name = SchemaUtil.normalizeIdentifier(name);
    }
    List<Integer> index = reverseIndex.get(name);
    if (index.isEmpty()) {
      if (!allCaseSensitive && someCaseSensitive) {
        name = SchemaUtil.normalizeIdentifier(name);
        index = reverseIndex.get(name);
      }
      if (index.isEmpty()) {
        throw new ColumnNotFoundException(name);
      }
    }

    return index.get(0);
  }

  public ColumnProjector getColumnProjector(int index) {
    return columnProjectors.get(index);
  }

  public int getColumnCount() {
    return columnProjectors.size();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("[");
    for (ColumnProjector projector : columnProjectors) {
      buf.append(projector.getExpression());
      buf.append(',');
    }
    if (buf.length() > 1) {
      buf.setLength(buf.length() - 1);
    }
    buf.append(']');
    return buf.toString();
  }

  public int getEstimatedRowByteSize() {
    return estimatedSize;
  }

  /**
   * allow individual expressions to reset their state between rows
   */
  public void reset() {
    for (ColumnProjector projector : columnProjectors) {
      projector.getExpression().reset();
    }
  }

  /**
   * PHOENIX-6644: Creates a new RowProjector with additional column name mappings merged from
   * another projector. This is useful when an optimized query (e.g., using an index) rewrites
   * column references but we want to preserve the original column names for
   * ResultSet.getString(columnName) compatibility. For example, when a view has "WHERE v1 = 'a'"
   * and an index is used, the optimizer may rewrite "SELECT v1 FROM view" to "SELECT 'a' FROM
   * index". This method adds the original column name "v1" to the reverseIndex so
   * ResultSet.getString("v1") continues to work.
   * @param sourceProjector the projector containing original column name mappings to preserve
   * @return a new RowProjector with merged column name mappings
   */
  public RowProjector mergeColumnNameMappings(RowProjector sourceProjector) {
    if (this.columnProjectors.size() != sourceProjector.columnProjectors.size()) {
      return this;
    }

    ListMultimap<String, Integer> additionalMappings = ArrayListMultimap.create();

    for (int i = 0; i < sourceProjector.columnProjectors.size(); i++) {
      ColumnProjector sourceCol = sourceProjector.columnProjectors.get(i);
      ColumnProjector currentCol = this.columnProjectors.get(i);

      // Only add source labels if they're different from current labels
      // This preserves original names like "v1" when optimizer rewrites to "'a'"
      String sourceLabel = sourceCol.getLabel();
      String currentLabel = currentCol.getLabel();

      if (!sourceLabel.equals(currentLabel)) {
        // Labels are different, so qualified names are guaranteed to be different
        additionalMappings.put(sourceLabel, i);
        if (!sourceCol.getTableName().isEmpty()) {
          additionalMappings.put(
            SchemaUtil.getColumnName(sourceCol.getTableName(), sourceCol.getLabel()), i);
        }
      } else {
        // Labels are the same, so check if table names differ
        if (!sourceCol.getTableName().isEmpty()
          && !sourceCol.getTableName().equals(currentCol.getTableName())) {
          additionalMappings.put(
            SchemaUtil.getColumnName(sourceCol.getTableName(), sourceCol.getLabel()), i);
        }
      }
    }

    // Create new RowProjector with additional mappings merged during construction
    // This maintains immutability.
    return new RowProjector(this.columnProjectors, this.estimatedSize, this.isProjectEmptyKeyValue,
      this.hasUDFs, this.isProjectAll, this.isProjectDynColsInWildcardQueries, additionalMappings);
  }
}
