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
package org.apache.phoenix.compile.keyspace;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;

/**
 * Walks a WHERE {@link Expression} tree bottom-up and produces the
 * {@link KeySpaceList} contribution of each node. The expression tree is expected to have
 * been pre-processed by {@link ExpressionNormalizer}, so RVC inequalities and scalar IN
 * lists have already been expanded into equivalent AND/OR trees over scalar comparisons.
 * The visitor therefore operates exclusively on the primitive shapes in the design doc's
 * model:
 * <ul>
 * <li>Scalar comparison on a PK column &rarr; one {@link KeySpace} with one non-EVERYTHING
 * dim.</li>
 * <li>Scalar comparison on a scalar function of a PK column &rarr; delegated to
 * {@code ScalarFunction.newKeyPart}.</li>
 * <li>LIKE &rarr; one {@link KeySpace} with a prefix range on the LHS dim.</li>
 * <li>IS [NOT] NULL &rarr; one {@link KeySpace} with IS_NULL / IS_NOT_NULL on the LHS dim.</li>
 * <li>RVC IN &rarr; one {@link KeySpace} per row value, each with per-dim equality ranges.</li>
 * <li>AND/OR &rarr; the list-level algebra on {@link KeySpaceList}.</li>
 * </ul>
 * Nodes that cannot be translated (non-PK columns, unsupported shapes) contribute
 * {@link KeySpaceList#everything(int)} — the identity for AND and the absorbing element for
 * OR — and are retained in the residual filter for correctness.
 */
public class KeySpaceExpressionVisitor
  extends StatelessTraverseNoExpressionVisitor<KeySpaceExpressionVisitor.Result> {

  /**
   * Result of visiting a sub-expression: the constraint it imposes and the set of
   * {@link Expression} nodes that have been fully absorbed into that constraint.
   */
  public static final class Result {
    final KeySpaceList list;
    final Set<Expression> consumed;

    public Result(KeySpaceList list, Set<Expression> consumed) {
      this.list = list;
      this.consumed = consumed;
    }

    public KeySpaceList list() {
      return list;
    }

    public Set<Expression> consumed() {
      return consumed;
    }

    static Result everything(int nPk) {
      return new Result(KeySpaceList.everything(nPk),
        java.util.Collections.<Expression>emptySet());
    }

    static Result unsatisfiable(int nPk) {
      return new Result(KeySpaceList.unsatisfiable(nPk),
        java.util.Collections.<Expression>emptySet());
    }
  }

  private final PTable table;
  private final int nPkColumns;

  public KeySpaceExpressionVisitor(PTable table) {
    this.table = table;
    this.nPkColumns = table.getPKColumns().size();
  }

  public int nPkColumns() {
    return nPkColumns;
  }

  // ------- visitEnter -------

  @Override
  public Iterator<Expression> visitEnter(AndExpression node) {
    return node.getChildren().iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(OrExpression node) {
    return node.getChildren().iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(ComparisonExpression node) {
    Expression rhs = node.getChildren().get(1);
    if (!rhs.isStateless() || node.getFilterOp() == CompareOperator.NOT_EQUAL) {
      return java.util.Collections.emptyIterator();
    }
    return java.util.Collections.singleton(node.getChildren().get(0)).iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(IsNullExpression node) {
    return java.util.Collections.singleton(node.getChildren().get(0)).iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(LikeExpression node) {
    if (node.getLikeType() == LikeType.CASE_INSENSITIVE
      || !(node.getChildren().get(1) instanceof LiteralExpression)
      || node.startsWithWildcard()) {
      return java.util.Collections.emptyIterator();
    }
    return java.util.Collections.singleton(node.getChildren().get(0)).iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(InListExpression node) {
    return java.util.Collections.singleton(node.getChildren().get(0)).iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
    return node.getChildren().iterator();
  }

  @Override
  public Iterator<Expression> visitEnter(
    org.apache.phoenix.expression.function.ArrayAnyComparisonExpression node) {
    // Don't descend into children — the ArrayElemRefExpression and its wrapper
    // ComparisonExpression don't correspond to extractable leaves on their own.
    // visitLeave handles the whole ArrayAny shape directly.
    return java.util.Collections.emptyIterator();
  }

  // ------- visit / visitLeave -------

  @Override
  public Result visit(RowKeyColumnExpression node) {
    return Result.everything(nPkColumns);
  }

  /**
   * {@code children} is filtered by {@code BaseExpression.acceptChildren}: null returns are
   * dropped and the order can differ from the declared AST order (PHOENIX-6669 sorts RVC
   * children first). Since AND is associative/commutative, index alignment is irrelevant
   * and branches that produced nothing are treated as the AND identity (everything).
   */
  @Override
  public Result visitLeave(AndExpression node, List<Result> children) {
    KeySpaceList acc = KeySpaceList.everything(nPkColumns);
    Set<Expression> consumed = new HashSet<>();
    boolean allChildrenFullyExtracted = true;
    int declaredChildren = node.getChildren().size();
    int returnedChildren = children == null ? 0 : children.size();
    if (returnedChildren < declaredChildren) {
      // Some children weren't visited at all (e.g. visitEnter bailed) — AND can't claim
      // it fully consumed its subtree.
      allChildrenFullyExtracted = false;
    }
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        Result r = children.get(i);
        if (r == null) {
          allChildrenFullyExtracted = false;
          continue;
        }
        acc = acc.and(r.list);
        consumed.addAll(r.consumed);
        // A child is fully extracted iff its consumed set includes the child expression
        // itself. Non-PK predicates return everything with empty consumed — those are
        // not fully extracted. Without this check, the AND's consumed nodes would get
        // propagated up past an OR that relies on the whole AND branch's truth value,
        // causing the residual stripper to remove PK predicates whose siblings were
        // unanalyzable — changing the OR's semantics.
        Expression childExpr = i < declaredChildren ? node.getChildren().get(i) : null;
        if (childExpr != null && !r.consumed.contains(childExpr)) {
          allChildrenFullyExtracted = false;
        }
        if (acc.isUnsatisfiable()) {
          break;
        }
      }
    }
    // If every AND child was fully extracted, the AND itself is fully extracted —
    // record the AND node so a parent OR can safely propagate the AND's consumed set.
    if (allChildrenFullyExtracted) {
      consumed.add(node);
    }
    return new Result(acc, consumed);
  }

  /**
   * OR semantics with a provable consumed rule.
   * <p>
   * <b>Invariant.</b> A node can be marked {@code consumed} — equivalently, stripped from
   * the residual filter — iff the emitted scan range matches exactly the same rows as the
   * original predicate:
   * <pre>rows(emit(node)) = rows(node)</pre>
   * For OR this holds only in specific shapes:
   * <ol>
   * <li><b>Singleton</b>: {@code acc.size() == 1}. The merged list is a single N-dim box.
   *     The per-slot extraction emits exactly that box; no information loss.</li>
   * <li><b>Single-dim</b>: every space in {@code acc} constrains only one dim (the same
   *     dim for every space), others being EVERYTHING. The per-slot projection on that
   *     dim carries the full union; other dims project to EVERYTHING.</li>
   * <li><b>Tautology</b>: {@code acc.isEverything()}. Emission matches all rows, predicate
   *     matches all rows.</li>
   * </ol>
   * In every other case — multi-space lists that constrain multiple dims, per-slot
   * projection loses the per-space correlation between dims. The OR node stays in the
   * residual filter so it gets re-evaluated server-side. No heuristics, no guesses.
   */
  @Override
  public Result visitLeave(OrExpression node, List<Result> children) {
    int declared = node.getChildren().size();
    int returned = children == null ? 0 : children.size();
    if (returned < declared) {
      // Some children weren't visited — be conservative, return EVERYTHING with empty
      // consumed so the residual filter retains the whole OR.
      return Result.everything(nPkColumns);
    }

    List<KeySpaceList> branchLists = new java.util.ArrayList<>(children.size());
    Set<Expression> branchConsumedUnion = new HashSet<>();
    boolean sawUnanalyzableBranch = false;
    boolean sawGenuineTautology = false;

    for (Result r : children) {
      if (r == null) {
        return Result.everything(nPkColumns);
      }
      if (r.list.isEverything()) {
        // Distinguish (a) genuine tautology (fully analyzed branch that happens to cover
        // all rows, e.g. `pk >= 7 OR pk < 9`) from (b) unanalyzable branch (e.g. non-PK
        // predicate). (a) has non-empty consumed, (b) has empty consumed.
        if (r.consumed != null && !r.consumed.isEmpty()) {
          sawGenuineTautology = true;
          branchConsumedUnion.addAll(r.consumed);
        } else {
          sawUnanalyzableBranch = true;
        }
        continue;
      }
      branchLists.add(r.list);
      if (r.consumed != null) {
        branchConsumedUnion.addAll(r.consumed);
      }
    }

    // Any unanalyzable branch means the OR is NOT a tautology and its truth value depends
    // on a predicate V2 can't extract; the residual filter must re-evaluate the whole OR.
    if (sawUnanalyzableBranch) {
      return Result.everything(nPkColumns);
    }

    // Compute `allBranchesFullyExtracted`: the OR's truth-preservation requires every
    // branch's own root expression to be in its consumed set. Without this check, the
    // residual-stripper could remove a leaf from one branch but leave a sibling's leaf,
    // changing the OR's semantics.
    boolean allBranchesFullyExtracted = true;
    for (int i = 0; i < children.size() && allBranchesFullyExtracted; i++) {
      Result r = children.get(i);
      Expression branchExpr = node.getChildren().get(i);
      if (r == null || !r.consumed.contains(branchExpr)) {
        allBranchesFullyExtracted = false;
      }
    }

    // Case 3 (tautology): any branch was a genuine tautology → whole OR is EVERYTHING.
    if (sawGenuineTautology) {
      if (allBranchesFullyExtracted) {
        Set<Expression> consumed = new HashSet<>(branchConsumedUnion);
        consumed.add(node);
        return new Result(KeySpaceList.everything(nPkColumns), consumed);
      }
      return Result.everything(nPkColumns);
    }

    KeySpaceList acc = KeySpaceList.orAll(nPkColumns, branchLists);

    if (!allBranchesFullyExtracted) {
      // Can't consume the OR; emit the narrowed scan range but leave OR in residual.
      return new Result(acc, java.util.Collections.<Expression>emptySet());
    }

    // Case 3 again (tautology via merging).
    if (acc.isEverything()) {
      Set<Expression> consumed = new HashSet<>(branchConsumedUnion);
      consumed.add(node);
      return new Result(acc, consumed);
    }

    // Case 1 (singleton): merged list is a single N-dim box.
    if (acc.size() == 1) {
      Set<Expression> consumed = new HashSet<>(branchConsumedUnion);
      consumed.add(node);
      return new Result(acc, consumed);
    }

    // Case 2 (single-dim): every space constrains exactly one dim, all the same dim.
    if (isSingleDimList(acc)) {
      Set<Expression> consumed = new HashSet<>(branchConsumedUnion);
      consumed.add(node);
      return new Result(acc, consumed);
    }

    // Multi-space, multi-dim OR: per-slot projection loses per-space dim correlation.
    // Emit narrowing, but the residual must re-evaluate the OR. This is the provably
    // correct handling of RVC lex-cascades and similar shapes.
    return new Result(acc, java.util.Collections.<Expression>emptySet());
  }

  /**
   * True iff every space in the list constrains exactly one dim, all spaces agreeing on
   * which dim that is. In that case the per-slot projection is exact — no information
   * loss when emitting {@link KeySpaceList} as per-slot ranges.
   */
  private static boolean isSingleDimList(KeySpaceList list) {
    int sharedDim = -1;
    for (KeySpace ks : list.spaces()) {
      int constrainedDim = -1;
      for (int d = 0; d < ks.nDims(); d++) {
        if (ks.get(d) != KeyRange.EVERYTHING_RANGE) {
          if (constrainedDim != -1) return false; // more than one constrained dim
          constrainedDim = d;
        }
      }
      if (constrainedDim == -1) return false; // fully-everything space shouldn't happen here
      if (sharedDim == -1) {
        sharedDim = constrainedDim;
      } else if (sharedDim != constrainedDim) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Result visitLeave(ComparisonExpression node, List<Result> children) {
    Expression lhs = node.getChildren().get(0);
    Expression rhs = node.getChildren().get(1);
    if (!rhs.isStateless() || node.getFilterOp() == CompareOperator.NOT_EQUAL) {
      return Result.everything(nPkColumns);
    }

    // Direct PK column on the LHS: emit a per-dim range. This is the primary case after
    // ExpressionNormalizer expanded RVC inequalities.
    Integer pkPos = pkPositionOf(lhs);
    if (pkPos != null) {
      PColumn column = table.getPKColumns().get(pkPos);
      KeyRange range = evalToKeyRange(node.getFilterOp(), rhs, column);
      if (range == null) {
        return Result.everything(nPkColumns);
      }
      KeySpace ks = KeySpace.single(pkPos, range, nPkColumns);
      KeySpaceList list = ks.isEmpty()
        ? KeySpaceList.unsatisfiable(nPkColumns)
        : KeySpaceList.of(ks);
      Set<Expression> consumed = new HashSet<>();
      consumed.add(node);
      return new Result(list, consumed);
    }

    // ScalarFunction(PK column) on the LHS: delegate to the function's KeyPart so
    // ROUND / CEIL / FLOOR / SUBSTR / TRIM can contribute a key range.
    ScalarFunctionChain chain = resolveScalarFunctionChain(lhs);
    if (chain == null) {
      return Result.everything(nPkColumns);
    }
    KeyRange range = chain.keyPart.getKeyRange(node.getFilterOp(), rhs);
    if (range == null) {
      return Result.everything(nPkColumns);
    }
    // Mirror V1's WhereOptimizer post-visit invert: when the underlying PK column is DESC,
    // the scalar-function KeyPart (e.g. PrefixFunction.PrefixKeyPart) already applies an
    // internal invert to the range. V1 then re-inverts once more before handing the slot
    // to ScanRanges.create, which expects DESC-encoded bytes in the slot's lower/upper
    // fields (the `inverted=true` flag gets stripped in ScanRanges' downstream path).
    // Without this second invert the bytes reach ScanRanges in their un-inverted form and
    // the resulting startRow/stopRow are ASC bytes instead of DESC, so the scan misses
    // all stored (DESC-encoded) rows. See SortOrderIT.substrVarLengthDescPK1.
    PColumn descColumn = table.getPKColumns().get(chain.pkPos);
    if (descColumn.getSortOrder() == org.apache.phoenix.schema.SortOrder.DESC) {
      range = range.invert();
    }
    KeySpace ks = KeySpace.single(chain.pkPos, range, nPkColumns);
    KeySpaceList list = ks.isEmpty()
      ? KeySpaceList.unsatisfiable(nPkColumns)
      : KeySpaceList.of(ks);
    Set<Expression> consumed = new HashSet<>();
    Set<Expression> partExtracts = chain.keyPart.getExtractNodes();
    // Only mark the comparison node as extracted when the scalar-function KeyPart signals
    // that the emitted range is semantically exact (getExtractNodes returns a non-empty
    // set containing the node it can safely extract). KeyParts that return an empty set
    // (e.g. RTrimFunction, which produces an over-permissive byte range that admits false
    // positives like 'b  a' for `rtrim(k) = 'b'`) require the residual filter to enforce
    // the original predicate per-row. Extracting the node in that case would drop the
    // residual and return wrong rows. See RTrimFunctionIT.testWithFixedLengthDescPK.
    if (partExtracts != null && !partExtracts.isEmpty()) {
      consumed.addAll(partExtracts);
      consumed.add(node);
    }
    return new Result(list, consumed);
  }

  @Override
  public Result visitLeave(IsNullExpression node, List<Result> children) {
    // Unwrap CoerceExpression wrappers for IS NULL / IS NOT NULL: type coercion doesn't
    // affect the null-semantics (NULL is NULL regardless of type), and Phoenix wraps the
    // index column reference in TO_<BASE_TYPE>() when the index column's stored type
    // differs from the base-table column. V1 handles this via CoerceKeyPart; we peel the
    // wrapper here so a predicate like `a_integer IS NOT NULL` on an index whose leading
    // column is wrapped as TO_INTEGER(a_integer) still narrows to IS_NOT_NULL_RANGE on the
    // inner PK column. See ReverseScanIT.testReverseScanIndex.
    Expression lhs = node.getChildren().get(0);
    while (lhs instanceof org.apache.phoenix.expression.CoerceExpression) {
      lhs = ((org.apache.phoenix.expression.CoerceExpression) lhs).getChildren().get(0);
    }
    Integer pkPos = pkPositionOf(lhs);
    if (pkPos == null) {
      return Result.everything(nPkColumns);
    }
    KeyRange range = node.isNegate() ? KeyRange.IS_NOT_NULL_RANGE : KeyRange.IS_NULL_RANGE;
    KeySpace ks = KeySpace.single(pkPos, range, nPkColumns);
    Set<Expression> consumed = new HashSet<>();
    consumed.add(node);
    return new Result(KeySpaceList.of(ks), consumed);
  }

  @Override
  public Result visitLeave(LikeExpression node, List<Result> children) {
    Expression lhs = node.getChildren().get(0);
    Integer pkPos = pkPositionOf(lhs);
    if (pkPos == null) {
      return Result.everything(nPkColumns);
    }
    if (node.getLikeType() == LikeType.CASE_INSENSITIVE
      || !(node.getChildren().get(1) instanceof LiteralExpression)
      || node.startsWithWildcard()) {
      return Result.everything(nPkColumns);
    }
    PColumn column = table.getPKColumns().get(pkPos);
    PDataType type = column.getDataType();
    String startsWith = node.getLiteralPrefix();
    byte[] key = PVarchar.INSTANCE.toBytes(startsWith, SortOrder.ASC);
    Integer lhsFixedLength = lhs.getDataType().isFixedWidth() ? lhs.getMaxLength() : null;
    if (lhsFixedLength != null && key.length > lhsFixedLength) {
      return Result.unsatisfiable(nPkColumns);
    }
    byte[] lowerRange = key;
    byte[] upperRange = ByteUtil.nextKey(key);
    Integer columnFixedLength = column.getMaxLength();
    if (type.isFixedWidth() && columnFixedLength != null) {
      lowerRange = type.pad(lowerRange, columnFixedLength, SortOrder.ASC);
      upperRange = type.pad(upperRange, columnFixedLength, SortOrder.ASC);
    }
    KeyRange range = type.getKeyRange(lowerRange, true, upperRange, false, SortOrder.ASC);
    if (lhs.getSortOrder() == SortOrder.DESC) {
      range = range.invert();
    }
    if (range == KeyRange.EMPTY_RANGE) {
      return Result.unsatisfiable(nPkColumns);
    }
    KeySpace ks = KeySpace.single(pkPos, range, nPkColumns);
    Set<Expression> consumed = new HashSet<>();
    if (node.endsWithOnlyWildcard()) {
      consumed.add(node);
    }
    return new Result(KeySpaceList.of(ks), consumed);
  }

  /**
   * RVC IN: {@code (c1,...,cK) IN ((v1a,...,vKa), (v1b,...,vKb), ...)}. Each row value
   * becomes a {@link KeySpace} with per-dim point equalities; the ORed list is the union of
   * those spaces. This faithfully represents the design's N-dimensional key-space model:
   * the LHS columns are distinct dimensions and each row value pins all of them.
   */
  @Override
  public Result visitLeave(InListExpression node, List<Result> children) {
    Expression lhs = node.getChildren().get(0);
    if (!(lhs instanceof RowValueConstructorExpression)) {
      // Scalar IN: `col IN (v1, v2, ...)`. Previously the ExpressionNormalizer rewrote
      // this to `col = v1 OR col = v2 OR ...` so the equality/OR visitor paths handled
      // it, but that rewrite changed the tree shape (callers saw OrExpression instead
      // of InListExpression) and wrapped literals in TO_VARCHAR coercions. Handle the
      // IN directly here: build one point KeySpace per value on the column's PK dim,
      // then union via orAll. Semantics identical to the OR rewrite; preserves the
      // InListExpression node in the tree.
      Integer pkPos = pkPositionOf(lhs);
      if (pkPos == null) {
        ScalarFunctionChain chain = resolveScalarFunctionChain(lhs);
        if (chain == null) {
          return Result.everything(nPkColumns);
        }
        return scalarInViaKeyPart(node, chain);
      }
      PColumn column = table.getPKColumns().get(pkPos);
      List<KeySpaceList> perValueLists =
        new java.util.ArrayList<>(node.getKeyExpressions().size());
      for (Expression v : node.getKeyExpressions()) {
        KeyRange range = evalToKeyRange(CompareOperator.EQUAL, v, column);
        if (range == null) {
          return Result.everything(nPkColumns);
        }
        if (range == KeyRange.EMPTY_RANGE) {
          continue;
        }
        perValueLists.add(KeySpaceList.of(KeySpace.single(pkPos, range, nPkColumns)));
      }
      KeySpaceList acc = KeySpaceList.orAll(nPkColumns, perValueLists);
      if (acc.isUnsatisfiable()) {
        return Result.unsatisfiable(nPkColumns);
      }
      Set<Expression> consumed = new HashSet<>();
      consumed.add(node);
      return new Result(acc, consumed);
    }
    RowValueConstructorExpression lhsRvc = (RowValueConstructorExpression) lhs;
    int lhsSize = lhsRvc.getChildren().size();
    int[] pkPositions = new int[lhsSize];
    // Per-child scalar-function chain: non-null when LHS child wraps a PK column in one or
    // more scalar functions (e.g., SUBSTR(parent_id, 1, 3)). Bare PK children leave the slot
    // null and are handled by the direct evalToKeyRange path. Mixing bare and wrapped
    // children in the same RVC is supported.
    ScalarFunctionChain[] chains = new ScalarFunctionChain[lhsSize];
    boolean anyChain = false;
    for (int i = 0; i < lhsSize; i++) {
      Expression child = lhsRvc.getChildren().get(i);
      Integer p = pkPositionOf(child);
      if (p != null) {
        pkPositions[i] = p;
        continue;
      }
      ScalarFunctionChain chain = resolveScalarFunctionChain(child);
      if (chain == null) {
        return Result.everything(nPkColumns);
      }
      chains[i] = chain;
      pkPositions[i] = chain.pkPos;
      anyChain = true;
    }

    // Collect per-row KeySpaces in bulk and union via a single KeySpaceList.orAll to
    // avoid the left-fold quadratic cost for large RVC-IN lists.
    List<KeySpaceList> perRowLists = new java.util.ArrayList<>(node.getKeyExpressions().size());
    for (Expression value : node.getKeyExpressions()) {
      KeySpace ks = buildRvcEqualitySpace(lhsRvc, value, pkPositions, chains);
      if (ks == null) {
        return Result.everything(nPkColumns);
      }
      if (ks.isEmpty()) {
        continue;
      }
      perRowLists.add(KeySpaceList.of(ks));
    }
    KeySpaceList acc = KeySpaceList.orAll(nPkColumns, perRowLists);
    if (acc.isUnsatisfiable()) {
      return Result.unsatisfiable(nPkColumns);
    }
    Set<Expression> consumed = new HashSet<>();
    // Only consume the RVC-IN if its LHS references a contiguous run of PK columns
    // starting from the first user PK (accounting for prefix columns like salt, viewIndexId,
    // and tenantId that the caller pins as a prefix). Otherwise the extractor's per-slot
    // fallback for middle-gap cases drops narrowing on the trailing dims while leaving
    // the node consumed, producing incorrect results with no residual filter to catch the
    // mismatch. Conservative check: consume only when the run starts at position <= 1
    // (covering global tables and multi-tenant with tenantId at position 0).
    //
    // When any LHS child is wrapped by a scalar function, the per-dim range may be a
    // strict subset of the child's full value set (e.g., SUBSTR(p,1,3)='abc' matches
    // any p starting with 'abc'). The residual filter must still evaluate the original
    // IN predicate, so we leave the node unconsumed whenever any chain is present.
    boolean isContiguous = true;
    int runStart = pkPositions[0];
    for (int i = 0; i < lhsSize; i++) {
      if (pkPositions[i] != runStart + i) {
        isContiguous = false;
        break;
      }
    }
    if (isContiguous && runStart <= 1 && !anyChain) {
      consumed.add(node);
    }
    return new Result(acc, consumed);
  }

  /**
   * ARRAY_ANY: {@code pk = ANY(array)} — semantically equivalent to {@code pk IN (...)}.
   * Mirrors V1's {@link org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor
   * #visitLeave(ArrayAnyComparisonExpression, List)}: iterate each array element and emit
   * a point {@link KeySpace} per element on the LHS PK column, then union via
   * {@link KeySpaceList#orAll}. Only the {@code col = ANY(literal-array)} shape is
   * handled; other shapes (non-PK LHS, non-EQUAL op, non-literal array, scalar-function
   * wrappers) fall through to EVERYTHING and keep the residual filter intact.
   * <p>
   * Without this, V2's visitor relied on the default no-op traversal for
   * {@code ArrayAnyComparisonExpression} — no KeySpace was produced, the scan was left as
   * full scan, and the query paid the cost of scanning every row to apply the residual.
   * See WhereOptimizerForArrayAnyIT tests.
   */
  @Override
  public Result visitLeave(
    org.apache.phoenix.expression.function.ArrayAnyComparisonExpression node,
    List<Result> children) {
    if (node.getChildren().size() != 2) {
      return Result.everything(nPkColumns);
    }
    Expression arrayExpr = node.getChildren().get(0);
    if (!(arrayExpr instanceof LiteralExpression)) {
      return Result.everything(nPkColumns);
    }
    Expression inner = node.getChildren().get(1);
    if (!(inner instanceof ComparisonExpression)) {
      return Result.everything(nPkColumns);
    }
    ComparisonExpression cmp = (ComparisonExpression) inner;
    if (cmp.getFilterOp() != CompareOperator.EQUAL) {
      return Result.everything(nPkColumns);
    }
    Expression cmpLhs = cmp.getChildren().get(0);
    Expression cmpRhs = cmp.getChildren().get(1);
    Expression pkRef = null;
    org.apache.phoenix.expression.function.ArrayElemRefExpression elemRef = null;
    if (cmpLhs instanceof RowKeyColumnExpression
      && cmpRhs instanceof org.apache.phoenix.expression.function.ArrayElemRefExpression) {
      pkRef = cmpLhs;
      elemRef = (org.apache.phoenix.expression.function.ArrayElemRefExpression) cmpRhs;
    } else if (cmpRhs instanceof RowKeyColumnExpression
      && cmpLhs instanceof org.apache.phoenix.expression.function.ArrayElemRefExpression) {
      pkRef = cmpRhs;
      elemRef = (org.apache.phoenix.expression.function.ArrayElemRefExpression) cmpLhs;
    } else {
      return Result.everything(nPkColumns);
    }
    if (elemRef.getChildren().isEmpty()
      || !(elemRef.getChildren().get(0) instanceof LiteralExpression)) {
      return Result.everything(nPkColumns);
    }
    Integer pkPos = pkPositionOf(pkRef);
    if (pkPos == null) {
      return Result.everything(nPkColumns);
    }
    PColumn column = table.getPKColumns().get(pkPos);
    org.apache.phoenix.schema.types.PhoenixArray arr =
      (org.apache.phoenix.schema.types.PhoenixArray) ((LiteralExpression) arrayExpr).getValue();
    if (arr == null) {
      return Result.everything(nPkColumns);
    }
    // Wrap the array element reference in a CoerceExpression so each element gets coerced
    // to the PK column's type (e.g. CHAR-padding, DESC inversion applied via the column's
    // SortOrder). This matches V1's handling.
    Expression coerceExpr;
    try {
      coerceExpr = org.apache.phoenix.expression.CoerceExpression.create(elemRef,
        column.getDataType(), column.getSortOrder(), column.getMaxLength());
    } catch (SQLException e) {
      return Result.everything(nPkColumns);
    }
    int numElements = arr.getDimensions();
    List<KeySpaceList> perValueLists = new java.util.ArrayList<>(numElements);
    for (int i = 1; i <= numElements; i++) {
      elemRef.setIndex(i);
      // Mirror V1's WhereOptimizer BaseKeyPart.getKeyRange: evaluate then pad fixed-width
      // types (with ASC pad character — DESC inversion is applied separately). A null
      // array element produces an empty-length ptr; for CHAR the pad fills with the space
      // character and yields a valid range of all-spaces. For truly-null elements on
      // variable-width types we skip after the eval yields empty bytes.
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      boolean evaluated = coerceExpr.evaluate(null, ptr);
      if (!evaluated) {
        continue;
      }
      PDataType type = column.getDataType();
      Integer length = column.getMaxLength();
      if (type.isFixedWidth() && length != null) {
        type.pad(ptr, length, SortOrder.ASC);
      } else if (ptr.getLength() == 0) {
        // Variable-width null — skip per SQL-standard ANY null semantics.
        continue;
      }
      byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
      KeyRange range = ByteUtil.getKeyRange(key, coerceExpr.getSortOrder(),
        CompareOperator.EQUAL, type);
      if (coerceExpr.getSortOrder() == SortOrder.DESC) {
        range = range.invert();
      }
      if (column.getSortOrder() == SortOrder.DESC) {
        range = range.invert();
      }
      if (range == null || range == KeyRange.EMPTY_RANGE || range == KeyRange.IS_NULL_RANGE) {
        continue;
      }
      perValueLists.add(KeySpaceList.of(KeySpace.single(pkPos, range, nPkColumns)));
    }
    if (perValueLists.isEmpty()) {
      return Result.everything(nPkColumns);
    }
    KeySpaceList acc = KeySpaceList.orAll(nPkColumns, perValueLists);
    if (acc.isUnsatisfiable()) {
      return Result.unsatisfiable(nPkColumns);
    }
    Set<Expression> consumed = new HashSet<>();
    consumed.add(node);
    return new Result(acc, consumed);
  }

  /**
   * Build a per-dim equality {@link KeySpace} for an IN-list row value. The value may be a
   * {@link RowValueConstructorExpression} of literals, or (after Phoenix's
   * {@code InListExpression.create} sort-and-coerce pass) a {@link LiteralExpression}
   * wrapping a packed compound byte array.
   * <p>
   * In the packed-literal case we split the bytes back into per-column pieces using the
   * column's fixed width or variable-length separator and assign each piece to the matching
   * PK dim. If the byte layout doesn't cleanly split (non-fixed-width with no separator),
   * we fall back to "everything" for that row.
   */
  private KeySpace buildRvcEqualitySpace(RowValueConstructorExpression lhs, Expression value,
    int[] pkPositions, ScalarFunctionChain[] chains) {
    KeyRange[] dims = new KeyRange[nPkColumns];
    java.util.Arrays.fill(dims, KeyRange.EVERYTHING_RANGE);

    if (value instanceof RowValueConstructorExpression) {
      RowValueConstructorExpression rhs = (RowValueConstructorExpression) value;
      int k = Math.min(lhs.getChildren().size(), rhs.getChildren().size());
      for (int i = 0; i < k; i++) {
        Expression rc = rhs.getChildren().get(i);
        KeyRange range;
        if (chains != null && chains[i] != null) {
          // Scalar-function child: delegate to the function's KeyPart so the byte
          // transforms (SUBSTR truncation, TO_CHAR encoding, etc.) and DESC inversion
          // are applied consistently with the scalar comparison path.
          range = chains[i].keyPart.getKeyRange(CompareOperator.EQUAL, rc);
        } else {
          PColumn column = table.getPKColumns().get(pkPositions[i]);
          range = evalToKeyRange(CompareOperator.EQUAL, rc, column);
        }
        if (range == null) {
          return null;
        }
        if (range == KeyRange.EMPTY_RANGE) {
          return KeySpace.empty(nPkColumns);
        }
        dims[pkPositions[i]] = range;
      }
      return KeySpace.of(dims);
    }

    // LiteralExpression packed compound bytes (InListExpression.create's sort path).
    // When any LHS child is scalar-function-wrapped, the packed bytes are pre-serialized
    // in the function's output type (InListExpression.create coerces RHS values to the
    // LHS children's types before packing). Split the bytes by the LHS child's declared
    // type width and feed each slice through the chain's KeyPart for range construction.
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    if (!value.evaluate(null, ptr) || ptr.getLength() == 0) {
      return null;
    }
    byte[] packed = ByteUtil.copyKeyBytesIfNecessary(ptr);
    int offset = 0;
    for (int i = 0; i < pkPositions.length; i++) {
      Expression lhsChild = lhs.getChildren().get(i);
      // Determine per-slice width from the LHS child's declared type — for
      // scalar-function children that's the function's output type (e.g., SUBSTR).
      PDataType sliceType = lhsChild.getDataType();
      Integer sliceMaxLen = lhsChild.getMaxLength();
      int len;
      if (sliceType != null && sliceType.isFixedWidth()) {
        len = (sliceMaxLen != null) ? sliceMaxLen : sliceType.getByteSize();
      } else {
        // Variable-width: scan to the next separator byte.
        int end = offset;
        while (end < packed.length && packed[end]
            != org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE) {
          end++;
        }
        len = end - offset;
      }
      if (offset + len > packed.length) {
        return null;
      }
      byte[] colBytes = new byte[len];
      System.arraycopy(packed, offset, colBytes, 0, len);
      KeyRange range;
      if (chains != null && chains[i] != null) {
        LiteralExpression lit;
        try {
          lit = LiteralExpression.newConstant(
            sliceType == null ? colBytes : sliceType.toObject(colBytes), sliceType);
        } catch (java.sql.SQLException sqe) {
          return null;
        }
        range = chains[i].keyPart.getKeyRange(CompareOperator.EQUAL, lit);
      } else {
        range = KeyRange.getKeyRange(colBytes, true, colBytes, true);
      }
      if (range == null) {
        return null;
      }
      if (range == KeyRange.EMPTY_RANGE) {
        return KeySpace.empty(nPkColumns);
      }
      dims[pkPositions[i]] = range;
      offset += len;
      if (sliceType != null && !sliceType.isFixedWidth() && offset < packed.length) {
        // Skip the separator byte between variable-width columns.
        offset++;
      }
    }
    return KeySpace.of(dims);
  }

  @Override
  public Result visitLeave(RowValueConstructorExpression node, List<Result> children) {
    // Bare RVC nodes reach this path only when they appear outside a ComparisonExpression
    // / InListExpression (unusual after normalization). Treat as everything.
    return Result.everything(nPkColumns);
  }

  // ------- helpers -------

  /** Returns the PK position if {@code e} is a PK {@link RowKeyColumnExpression}; else null. */
  private Integer pkPositionOf(Expression e) {
    if (e instanceof RowKeyColumnExpression) {
      int pos = ((RowKeyColumnExpression) e).getPosition();
      if (pos >= 0 && pos < nPkColumns) {
        return pos;
      }
    }
    return null;
  }

  /**
   * Evaluates {@code rhs} into a per-column {@link KeyRange} for the given PK column.
   * <p>
   * If the PK column is stored DESC, the emitted range is DESC-inverted so the scan
   * machinery in {@link KeyRangeExtractor} / {@link org.apache.phoenix.compile.ScanRanges}
   * sees bytes in the physical storage order. Without this, a query like
   * {@code OBJECT_VERSION IN ('1111', '2222')} on a DESC PK would produce ASC-encoded
   * ranges that don't match the DESC-sorted HBase rows — the scan would either miss
   * rows or over-scan. V1 applies the same inversion in
   * {@code WhereOptimizer.pushKeyExpressionsToScan} after visitor collection; we bake
   * it into the visitor so downstream list-merging operates on the physical-order
   * bytes throughout.
   */
  private KeyRange evalToKeyRange(CompareOperator op, Expression rhs, PColumn column) {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    if (!rhs.evaluate(null, ptr) || ptr.getLength() == 0) {
      return null;
    }
    PDataType type = column.getDataType();
    if (type.isFixedWidth()) {
      Integer length = column.getMaxLength();
      if (length != null) {
        type.pad(ptr, length, SortOrder.ASC);
      }
    }
    byte[] key = ByteUtil.copyKeyBytesIfNecessary(ptr);
    KeyRange range = ByteUtil.getKeyRange(key, rhs.getSortOrder(), op, type);
    if (rhs.getSortOrder() == SortOrder.DESC) {
      range = range.invert();
    }
    if (column.getSortOrder() == SortOrder.DESC) {
      range = range.invert();
    }
    return range;
  }

  /**
   * Scalar {@code IN (v1, v2, ...)} with a scalar-function wrapper on the LHS
   * (e.g. {@code SUBSTR(pk_col, 1, 3) IN ('foo', 'bar')}). Each value becomes a
   * point range on the inner PK column via the function's key-part chain.
   */
  private Result scalarInViaKeyPart(InListExpression node, ScalarFunctionChain chain) {
    List<KeySpaceList> perValueLists = new java.util.ArrayList<>(node.getKeyExpressions().size());
    for (Expression v : node.getKeyExpressions()) {
      KeyRange range = chain.keyPart.getKeyRange(CompareOperator.EQUAL, v);
      if (range == null) {
        return Result.everything(nPkColumns);
      }
      if (range == KeyRange.EMPTY_RANGE) {
        continue;
      }
      perValueLists.add(KeySpaceList.of(KeySpace.single(chain.pkPos, range, nPkColumns)));
    }
    KeySpaceList acc = KeySpaceList.orAll(nPkColumns, perValueLists);
    if (acc.isUnsatisfiable()) {
      return Result.unsatisfiable(nPkColumns);
    }
    Set<Expression> consumed = new HashSet<>();
    consumed.add(node);
    Set<Expression> partExtracts = chain.keyPart.getExtractNodes();
    if (partExtracts != null) {
      consumed.addAll(partExtracts);
    }
    return new Result(acc, consumed);
  }

  /** Chain of scalar functions resolved to an inner PK column. */
  private static final class ScalarFunctionChain {
    final int pkPos;
    final org.apache.phoenix.compile.KeyPart keyPart;

    ScalarFunctionChain(int pkPos, org.apache.phoenix.compile.KeyPart keyPart) {
      this.pkPos = pkPos;
      this.keyPart = keyPart;
    }
  }

  /**
   * Walks a chain of {@link org.apache.phoenix.expression.function.ScalarFunction} nodes
   * down to an inner {@link RowKeyColumnExpression}, composing a
   * {@link org.apache.phoenix.compile.KeyPart} at each level.
   * <p>
   * Bare {@link org.apache.phoenix.expression.CoerceExpression} wrappers (no enclosing
   * ScalarFunction) are also handled: Phoenix inserts these when an index PK column's
   * stored type differs from the column's logical type (e.g. {@code INT_COL1} on an
   * index where the leading PK is stored as VARBINARY). The resulting
   * {@link CoerceKeyPart} reverse-translates the RHS bytes through the coerce's inner
   * type before emitting the scan bounds, matching V1's
   * {@code WhereOptimizer.KeyExpressionVisitor.newCoerceKeyPart} behavior.
   * <p>
   * CoerceExpression <b>inside</b> a ScalarFunction chain (e.g.
   * {@code TO_CHAR(CoerceExpression(col))}) is NOT unwrapped here — V1's ScalarFunction
   * KeyParts operate on the outer, post-coerce form and V2 matches that.
   */
  private ScalarFunctionChain resolveScalarFunctionChain(Expression node) {
    // Bare CoerceExpression wrapping an ASC PK column: handle specifically. This is the
    // {@code TO_INTEGER(INT_COL1) = 2} pattern on uncovered-index plans, where Phoenix
    // wraps the index-stored column in a CoerceExpression. The DESC variant is
    // deliberately excluded — V1's DESC-RVC tests pin V2 at a non-extracted shape, and
    // extending coerce-unwrap to DESC would change that behavior.
    if (node instanceof org.apache.phoenix.expression.CoerceExpression
      && !node.getChildren().isEmpty()
      && node.getChildren().get(0) instanceof RowKeyColumnExpression) {
      org.apache.phoenix.expression.CoerceExpression coerce =
        (org.apache.phoenix.expression.CoerceExpression) node;
      Expression inner = coerce.getChildren().get(0);
      Integer pkPos = pkPositionOf(inner);
      if (pkPos == null) {
        return null;
      }
      PColumn column = table.getPKColumns().get(pkPos);
      if (column.getSortOrder() == org.apache.phoenix.schema.SortOrder.DESC) {
        return null;
      }
      org.apache.phoenix.compile.KeyPart base =
        new org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor.BaseKeyPart(table,
          column, new LinkedHashSet<Expression>(
            java.util.Collections.<Expression>singletonList(inner)));
      return new ScalarFunctionChain(pkPos, new CoerceKeyPart(base, coerce));
    }
    if (!(node instanceof org.apache.phoenix.expression.function.ScalarFunction)) {
      return null;
    }
    java.util.Deque<org.apache.phoenix.expression.function.ScalarFunction> stack =
      new java.util.ArrayDeque<>();
    Expression cur = node;
    while (cur instanceof org.apache.phoenix.expression.function.ScalarFunction) {
      org.apache.phoenix.expression.function.ScalarFunction fn =
        (org.apache.phoenix.expression.function.ScalarFunction) cur;
      int idx = fn.getKeyFormationTraversalIndex();
      if (idx < 0 || idx >= fn.getChildren().size()) {
        return null;
      }
      stack.push(fn);
      cur = fn.getChildren().get(idx);
    }
    Integer pkPos = pkPositionOf(cur);
    if (pkPos == null) {
      return null;
    }
    PColumn column = table.getPKColumns().get(pkPos);
    org.apache.phoenix.compile.KeyPart part =
      new org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor.BaseKeyPart(table,
        column, new LinkedHashSet<Expression>(
          java.util.Collections.<Expression>singletonList(cur)));
    while (!stack.isEmpty()) {
      org.apache.phoenix.expression.function.ScalarFunction fn = stack.pop();
      org.apache.phoenix.compile.KeyPart wrapped = fn.newKeyPart(part);
      if (wrapped == null) {
        return null;
      }
      part = wrapped;
    }
    return new ScalarFunctionChain(pkPos, part);
  }

  /**
   * V2 replica of {@code WhereOptimizer.KeyExpressionVisitor.CoerceKeySlot} (which is
   * package-private and can't be reached from here). Wraps an inner {@link KeyPart} with
   * a reverse byte-coercion so the scan's key range is in the inner column's stored byte
   * form.
   */
  private static final class CoerceKeyPart implements org.apache.phoenix.compile.KeyPart {
    private final org.apache.phoenix.compile.KeyPart childPart;
    private final org.apache.phoenix.expression.CoerceExpression node;
    private final Set<Expression> extractNodes;

    CoerceKeyPart(org.apache.phoenix.compile.KeyPart childPart,
      org.apache.phoenix.expression.CoerceExpression node) {
      this.childPart = childPart;
      this.node = node;
      this.extractNodes = new LinkedHashSet<Expression>(
        java.util.Collections.<Expression>singletonList(node));
    }

    @Override
    public KeyRange getKeyRange(CompareOperator op, Expression rhs) {
      KeyRange range = childPart.getKeyRange(op, rhs);
      if (range == null) {
        return null;
      }
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      byte[] lower = range.getLowerRange();
      if (!range.lowerUnbound()) {
        ptr.set(lower);
        node.getChild().getDataType().coerceBytes(ptr, node.getDataType(), rhs.getSortOrder(),
          SortOrder.ASC);
        lower = ByteUtil.copyKeyBytesIfNecessary(ptr);
      }
      byte[] upper = range.getUpperRange();
      if (!range.upperUnbound()) {
        ptr.set(upper);
        node.getChild().getDataType().coerceBytes(ptr, node.getDataType(), rhs.getSortOrder(),
          SortOrder.ASC);
        upper = ByteUtil.copyKeyBytesIfNecessary(ptr);
      }
      return KeyRange.getKeyRange(lower, range.isLowerInclusive(), upper,
        range.isUpperInclusive());
    }

    @Override
    public Set<Expression> getExtractNodes() {
      return extractNodes;
    }

    @Override
    public PColumn getColumn() {
      return childPart.getColumn();
    }

    @Override
    public PTable getTable() {
      return childPart.getTable();
    }
  }
}
