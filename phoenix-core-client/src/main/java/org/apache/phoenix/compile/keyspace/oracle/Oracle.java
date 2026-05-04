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
package org.apache.phoenix.compile.keyspace.oracle;

/**
 * Reference implementation (oracle) of the key-space model's key-range extraction
 * algorithm. Given an {@link AbstractExpression} tree over a schema with {@code nPk}
 * primary-key dimensions, produces the {@link AbstractKeySpaceList} the algorithm
 * should emit.
 * <p>
 * The purpose is differential testing: we compare the oracle's output against the
 * production {@code WhereOptimizerV2} implementation's {@code KeySpaceList} to detect
 * divergences. Any difference is either a production bug or an oracle bug — the oracle
 * being shorter and directly derived from the design, the default suspect is production.
 * <p>
 * This oracle does not handle:
 * <ul>
 * <li>Normalization of RVC inequalities (feed in the lex-expanded form).</li>
 * <li>Byte encoding, DESC inversion, separator bytes, salt/tenant prefixes.</li>
 * <li>Null handling (IS NULL / IS NOT NULL).</li>
 * <li>Scalar function wrappers or coercions.</li>
 * </ul>
 * All of those are production concerns that live above the algebra the model describes.
 * <p>
 * <b>Correctness property.</b> For every row {@code r} where
 * {@code expr.evaluate(r) == true}, the emitted {@link AbstractKeySpaceList} must match
 * {@code r} (soundness: no false negatives). False positives — rows in the list but not
 * satisfying the expression — are permitted because the production residual filter
 * re-evaluates the original predicate at scan time.
 */
public final class Oracle {

  private Oracle() {}

  /**
   * Default cartesian bound used by {@link #extract(AbstractExpression, int)}. Matches
   * production's order of magnitude; tests that want a tighter bound for explosion
   * behavior should call the two-arg overload.
   */
  public static final int DEFAULT_CARTESIAN_BOUND = 50_000;

  public static AbstractKeySpaceList extract(AbstractExpression expr, int nPk) {
    return extract(expr, nPk, DEFAULT_CARTESIAN_BOUND);
  }

  /**
   * Recursively converts {@code expr} into a {@link AbstractKeySpaceList} per the
   * key-space algorithm:
   * <ol>
   * <li>Leaf {@code Pred} → singleton list containing a {@link AbstractKeySpace} with
   * EVERYTHING on every dim except the leaf's dim, which carries the comparison range.</li>
   * <li>{@code And} → cross-product intersection, then merge-to-fixpoint.</li>
   * <li>{@code Or} → concat, then merge-to-fixpoint.</li>
   * </ol>
   * After each list-producing step, if the list size exceeds {@code cartesianBound}, apply
   * the "drop trailing dims" widening rule until the list fits. This preserves the
   * O(N²) complexity bound.
   */
  public static AbstractKeySpaceList extract(AbstractExpression expr, int nPk, int cartesianBound) {
    AbstractKeySpaceList raw = toKeySpaceList(expr, nPk);
    while (raw.size() > cartesianBound && !raw.isUnsatisfiable() && !raw.isEverything()) {
      AbstractKeySpaceList narrower = raw.dropTrailingDim();
      if (narrower.size() >= raw.size()) break; // no further progress — bail
      raw = narrower;
    }
    return raw;
  }

  private static AbstractKeySpaceList toKeySpaceList(AbstractExpression expr, int nPk) {
    if (expr instanceof AbstractExpression.Unknown) {
      // Unanalyzable leaf — contributes no narrowing. Treated as `true` everywhere, so
      // the emitted KeySpaceList is the AND identity (everything).
      return AbstractKeySpaceList.everything(nPk);
    }
    if (expr instanceof AbstractExpression.Pred) {
      AbstractExpression.Pred p = (AbstractExpression.Pred) expr;
      AbstractRange<?> r = rangeFor(p.op, p.value);
      if (r.isEmpty()) return AbstractKeySpaceList.unsatisfiable(nPk);
      return AbstractKeySpaceList.of(nPk, AbstractKeySpace.single(p.dim, r, nPk));
    }
    if (expr instanceof AbstractExpression.And) {
      AbstractExpression.And a = (AbstractExpression.And) expr;
      AbstractKeySpaceList acc = AbstractKeySpaceList.everything(nPk);
      for (AbstractExpression c : a.children) {
        acc = acc.and(toKeySpaceList(c, nPk));
        if (acc.isUnsatisfiable()) return acc;
      }
      return acc;
    }
    if (expr instanceof AbstractExpression.Or) {
      AbstractExpression.Or o = (AbstractExpression.Or) expr;
      AbstractKeySpaceList acc = AbstractKeySpaceList.unsatisfiable(nPk);
      for (AbstractExpression c : o.children) {
        acc = acc.or(toKeySpaceList(c, nPk));
        if (acc.isEverything()) return acc;
      }
      return acc;
    }
    throw new IllegalArgumentException("unknown expression kind: " + expr.getClass());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractRange<?> rangeFor(AbstractExpression.Op op, Comparable value) {
    switch (op) {
      case EQ: return AbstractRange.point(value);
      case LT: return AbstractRange.lessThan(value);
      case LE: return AbstractRange.atMost(value);
      case GT: return AbstractRange.greaterThan(value);
      case GE: return AbstractRange.atLeast(value);
      default: throw new IllegalStateException();
    }
  }
}
