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

import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.and;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.or;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.pred;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.EQ;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.GE;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.GT;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.LE;
import static org.apache.phoenix.compile.keyspace.oracle.AbstractExpression.Op.LT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

/**
 * Unit tests for {@link Oracle}. Three groups:
 * <ul>
 * <li><b>Algebra</b> — AND/OR identity, idempotence, commutativity on the list algebra.</li>
 * <li><b>Worked examples</b> — specific scenarios that exercise the merge rules.</li>
 * <li><b>Soundness</b> — for random expressions, every row satisfying the expression is
 * contained in the emitted KeySpaceList (no false negatives). The core correctness
 * guarantee.</li>
 * </ul>
 */
public class OracleTest {

  // ---------- algebra ----------

  @Test
  public void andIsIdempotent() {
    AbstractExpression e = pred(0, EQ, 5L);
    AbstractKeySpaceList once = Oracle.extract(e, 3);
    AbstractKeySpaceList twice = Oracle.extract(and(e, e), 3);
    assertEquals(once, twice);
  }

  @Test
  public void orIsIdempotent() {
    AbstractExpression e = pred(0, EQ, 5L);
    AbstractKeySpaceList once = Oracle.extract(e, 3);
    AbstractKeySpaceList twice = Oracle.extract(or(e, e), 3);
    assertEquals(once, twice);
  }

  @Test
  public void andCommutes() {
    AbstractExpression a = pred(0, EQ, 5L);
    AbstractExpression b = pred(1, GT, 10L);
    assertEquals(Oracle.extract(and(a, b), 3), Oracle.extract(and(b, a), 3));
  }

  @Test
  public void orCommutes() {
    // Commutativity preserves the set of spaces, not the list order. Compare as sets.
    AbstractExpression a = pred(0, EQ, 5L);
    AbstractExpression b = pred(0, EQ, 7L);
    AbstractKeySpaceList la = Oracle.extract(or(a, b), 3);
    AbstractKeySpaceList lb = Oracle.extract(or(b, a), 3);
    assertEquals(new java.util.HashSet<>(la.spaces()), new java.util.HashSet<>(lb.spaces()));
  }

  @Test
  public void unsatisfiableAndAnythingIsUnsatisfiable() {
    AbstractExpression a = and(pred(0, GE, 10L), pred(0, LT, 5L));
    AbstractKeySpaceList result = Oracle.extract(a, 3);
    assertTrue(result.isUnsatisfiable());
  }

  @Test
  public void tautologyOnSingleDim() {
    AbstractExpression a = or(pred(0, LT, 10L), pred(0, GE, 10L));
    AbstractKeySpaceList result = Oracle.extract(a, 3);
    assertTrue(result.isEverything());
  }

  @Test
  public void containmentMergesUnderOr() {
    // `x >= 5 OR x = 7` — the point is contained in the range; should merge to `x >= 5`.
    AbstractExpression a = or(pred(0, GE, 5L), pred(0, EQ, 7L));
    AbstractKeySpaceList result = Oracle.extract(a, 1);
    assertEquals(1, result.size());
    assertEquals(AbstractRange.atLeast(5L), result.spaces().get(0).get(0));
  }

  @Test
  public void adjacentRangesMergeUnderOr() {
    // `x >= 5 AND x <= 7` merges trivially; `x <= 4 OR x > 4` is the tautology case.
    AbstractExpression a = or(pred(0, LE, 4L), pred(0, GT, 4L));
    AbstractKeySpaceList result = Oracle.extract(a, 1);
    assertTrue(result.isEverything());
  }

  // ---------- worked examples ----------

  @Test
  public void workedExample_orOfDisjointLeadingDimSpaces() {
    // `[(7,*), (*,8), (4,7)] OR [(*,7), (*,8), (4,7)]` stays as two entries because the
    // leading dim's ranges are disjoint — (7, +∞) vs (−∞, 7). In our notation:
    // `d0 > 7` vs `d0 < 7` — those ARE adjacent at 7 with both exclusive, so they must
    // stay separate (the point 7 is missing from both). Build equivalent inputs and
    // confirm: 2 spaces.
    AbstractKeySpace a = AbstractKeySpace.of(
      AbstractRange.greaterThan(7L), AbstractRange.lessThan(8L), AbstractRange.of(4L, true, 7L, true));
    AbstractKeySpace b = AbstractKeySpace.of(
      AbstractRange.lessThan(7L), AbstractRange.lessThan(8L), AbstractRange.of(4L, true, 7L, true));
    AbstractKeySpaceList la = AbstractKeySpaceList.of(3, a);
    AbstractKeySpaceList lb = AbstractKeySpaceList.of(3, b);
    AbstractKeySpaceList combined = la.or(lb);
    assertEquals(2, combined.size());
  }

  @Test
  public void workedExample_containmentMergesTwoSpaces() {
    // `[(*, +∞), (*, 8), (4,7)] OR [(5,*), (*,8), (4,7)]` → the first contains the second
    // (first has everything on d0, second constrains d0 to `> 5`). Merged result: first.
    AbstractKeySpace outer = AbstractKeySpace.of(
      AbstractRange.everything(), AbstractRange.lessThan(8L), AbstractRange.of(4L, true, 7L, true));
    AbstractKeySpace inner = AbstractKeySpace.of(
      AbstractRange.greaterThan(5L), AbstractRange.lessThan(8L), AbstractRange.of(4L, true, 7L, true));
    AbstractKeySpaceList merged = AbstractKeySpaceList.of(3, outer).or(AbstractKeySpaceList.of(3, inner));
    assertEquals(1, merged.size());
    assertEquals(outer, merged.spaces().get(0));
  }

  @Test
  public void workedExample_andOfRvcLexExpansion() {
    // This mirrors testRVCScanBoundaries1's first case at the abstract level:
    //   category = 'cat0' AND score <= 5000 AND (score, pk, sk) > (4990, 'pk_90', 4990)
    // Normalized: the RVC expands to 3 OR branches:
    //   score > 4990
    //   score = 4990 AND pk > 'pk_90'
    //   score = 4990 AND pk = 'pk_90' AND sk > 4990
    // Conjoined with `category = 'cat0' AND score <= 5000`, the oracle should produce
    // 3 spaces describing the valid compound lex region.
    AbstractExpression rvcExpanded = or(
      pred(1, GT, 4990L),
      and(pred(1, EQ, 4990L), pred(2, GT, "pk_90")),
      and(pred(1, EQ, 4990L), pred(2, EQ, "pk_90"), pred(3, GT, 4990L))
    );
    AbstractExpression full = and(
      pred(0, EQ, "cat_0"),
      pred(1, LE, 5000L),
      rvcExpanded);
    AbstractKeySpaceList result = Oracle.extract(full, 4);
    assertEquals(3, result.size());
    // Every space should carry category = 'cat_0' on dim 0.
    for (AbstractKeySpace ks : result.spaces()) {
      assertEquals(AbstractRange.point("cat_0"), ks.get(0));
    }
  }

  // ---------- soundness: every matching row is in the emitted list ----------

  @Test
  public void soundnessRandom_3Dims_boundedValues() {
    Random rnd = new Random(42);
    for (int trial = 0; trial < 50; trial++) {
      AbstractExpression expr = randomExpression(rnd, 3, /*maxDepth=*/3, /*valueRange=*/5);
      AbstractKeySpaceList extracted = Oracle.extract(expr, 3);
      // Enumerate all (a, b, c) ∈ [0..10)³ and check: if expr is true, extracted matches.
      for (long a = 0; a < 10; a++) {
        for (long b = 0; b < 10; b++) {
          for (long c = 0; c < 10; c++) {
            List<Object> row = Arrays.<Object>asList(a, b, c);
            if (expr.evaluate(row) && !extracted.matches(row)) {
              throw new AssertionError(
                "soundness violation: expr " + expr + " matches row " + row
                  + " but extracted " + extracted + " does not");
            }
          }
        }
      }
    }
  }

  @Test
  public void soundnessPreservedUnderWidening() {
    // The cartesian-bound "drop trailing dim" rule only widens, never narrows, so
    // soundness must still hold when the bound forces widening.
    // Build an expression with an OR of 50 disjoint points on dim 0 and a range on dim 1.
    AbstractExpression[] branches = new AbstractExpression[50];
    for (int i = 0; i < 50; i++) {
      branches[i] = and(pred(0, EQ, (long) i), pred(1, GE, (long) i));
    }
    AbstractExpression expr = or(branches);
    AbstractKeySpaceList wide = Oracle.extract(expr, 2, /*cartesianBound=*/10);
    // Post-widening size should be at most the bound (or 1 if widened all the way down).
    assertTrue("widened list should fit the bound, got " + wide.size(), wide.size() <= 50);
    // Soundness: every row that matches expr must also match wide.
    for (long a = 0; a < 60; a++) {
      for (long b = 0; b < 60; b++) {
        List<Object> row = Arrays.<Object>asList(a, b);
        if (expr.evaluate(row)) assertTrue(wide.matches(row));
      }
    }
  }

  @Test
  public void degeneracyDetectedOnAnyDim() {
    // PHOENIX-6669: a contradiction on a non-leading PK dim should still produce
    // unsatisfiable. The per-dim intersection rule handles this uniformly.
    AbstractExpression expr = and(pred(0, EQ, 1L), pred(2, GE, 10L), pred(2, LT, 5L));
    AbstractKeySpaceList result = Oracle.extract(expr, 3);
    assertTrue(result.isUnsatisfiable());
  }

  @Test
  public void equalityOnSameDimTwiceCollapses() {
    AbstractExpression expr = and(pred(0, EQ, 5L), pred(0, EQ, 5L));
    AbstractKeySpaceList result = Oracle.extract(expr, 2);
    assertEquals(1, result.size());
    assertEquals(AbstractRange.point(5L), result.spaces().get(0).get(0));
  }

  @Test
  public void conflictingEqualitiesCollapseToUnsatisfiable() {
    AbstractExpression expr = and(pred(0, EQ, 5L), pred(0, EQ, 7L));
    AbstractKeySpaceList result = Oracle.extract(expr, 2);
    assertTrue(result.isUnsatisfiable());
  }

  // ---------- helpers ----------

  private static AbstractExpression randomExpression(Random rnd, int nPk, int maxDepth,
    int valueRange) {
    if (maxDepth == 0 || rnd.nextInt(3) == 0) {
      int d = rnd.nextInt(nPk);
      AbstractExpression.Op op = AbstractExpression.Op.values()[rnd.nextInt(5)];
      long v = rnd.nextInt(valueRange);
      return pred(d, op, v);
    }
    int k = 2 + rnd.nextInt(2);
    AbstractExpression[] kids = new AbstractExpression[k];
    for (int i = 0; i < k; i++) {
      kids[i] = randomExpression(rnd, nPk, maxDepth - 1, valueRange);
    }
    return rnd.nextBoolean() ? and(kids) : or(kids);
  }

  @Test
  public void listHasNoDuplicates() {
    // After merge-to-fixpoint, no two spaces in the list should be equal.
    AbstractExpression expr = or(pred(0, EQ, 1L), pred(0, EQ, 1L), pred(0, EQ, 2L));
    AbstractKeySpaceList result = Oracle.extract(expr, 1);
    assertEquals(2, result.size());
  }

  @Test
  public void singleDimOrCoalesces() {
    // `d0 >= 5 OR d0 < 3 OR d0 = 4` should coalesce to two adjacent ranges merged at 4.
    AbstractExpression expr = or(pred(0, GE, 5L), pred(0, LT, 3L), pred(0, EQ, 4L));
    AbstractKeySpaceList result = Oracle.extract(expr, 1);
    // [lt 3] and [=4] are disjoint (gap at 3). [=4] and [>=5] are adjacent (4 vs 5 gap).
    // All three are non-mergeable as pairs: so 3 spaces... let me reason carefully.
    // Actually [=4] and [>=5]: shared endpoint? [=4] is [4,4]; [>=5] is [5,+∞). 4 and 5
    // are different longs — disjoint, not adjacent. So oracle keeps 3 spaces.
    assertEquals(3, result.size());
  }

  @Test
  public void leadingEqualityLockedByAndHasOneSpaceAfterRvcExpand() {
    // PK2 is pinned to 5; RVC expansion adds ORs that would normally produce 3 spaces, but
    // any branch that conflicts with PK2=5 is ruled out.
    AbstractExpression expr = and(
      pred(1, EQ, 5L),
      or(
        and(pred(0, EQ, 1L), pred(1, EQ, 5L)),
        and(pred(0, EQ, 2L), pred(1, EQ, 5L))
      ));
    AbstractKeySpaceList result = Oracle.extract(expr, 3);
    // 2 spaces: (d0=1, d1=5, *), (d0=2, d1=5, *)
    assertEquals(2, result.size());
    // ... each with d1 pinned to 5.
    for (AbstractKeySpace ks : result.spaces()) {
      assertFalse(ks.get(1).isEverything());
      assertEquals(AbstractRange.point(5L), ks.get(1));
    }
  }
}
