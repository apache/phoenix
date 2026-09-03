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
package org.apache.phoenix.compile.keyspace.algebra;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Builds a row-enumeration grid for a given {@link AbstractExpression} tree. Walks the
 * tree collecting per-dim literal values, then for each dim produces a small candidate set
 * <em>around</em> those literals — literal-ε, literal, literal+ε — so every boundary
 * condition in the predicate gets exercised by enumeration.
 * <p>
 * The strategy is type-aware:
 * <ul>
 * <li>{@link Long} / {@link Integer}: literals themselves plus ±1.</li>
 * <li>{@link BigDecimal}: literals plus ±1.</li>
 * <li>{@link String}: literals plus one character less (`"pk_9"` → `"pk_8"`, `"pk_9"`,
 * `"pk_a"`) via character increment/decrement on the last char. Strings are awkward
 * because the "next string" depends on ordering, so we use: literal, stripped-last-char,
 * last-char-plus-one (where meaningful).</li>
 * </ul>
 * For dims with no literal in the expression (everything-everything cases), we include a
 * single sentinel value per type so enumeration still produces rows.
 * <p>
 * Grid size is bounded by {@code maxPerDim^nPk}. Callers should check via
 * {@link #estimateSize} before building.
 */
public final class EnumerationGrid {

  private EnumerationGrid() {}

  /**
   * Build a per-dim list of candidate values. Each sublist is sorted and deduplicated.
   * Empty dims (no literal seen) get one placeholder.
   */
  public static List<List<Object>> build(AbstractExpression expr, int nPk) {
    List<Set<Object>> perDim = new ArrayList<>(nPk);
    for (int i = 0; i < nPk; i++) {
      perDim.add(new HashSet<Object>());
    }
    collect(expr, perDim);

    List<List<Object>> out = new ArrayList<>(nPk);
    for (int i = 0; i < nPk; i++) {
      Set<Object> values = perDim.get(i);
      if (values.isEmpty()) {
        // Dim has no literal; use a single placeholder. Pick a Long — works for numeric
        // dims and enumerates as anything for unconstrained dims.
        out.add(java.util.Collections.<Object>singletonList(0L));
        continue;
      }
      Set<Object> expanded = new TreeSet<>(AnyComparator.INSTANCE);
      for (Object v : values) {
        expanded.add(v);
        Object minus = perturbDown(v);
        if (minus != null) expanded.add(minus);
        Object plus = perturbUp(v);
        if (plus != null) expanded.add(plus);
      }
      out.add(Collections.<Object>unmodifiableList(new ArrayList<>(expanded)));
    }
    return out;
  }

  /** Estimate the grid size (product of per-dim sublist sizes). */
  public static long estimateSize(List<List<Object>> grid) {
    long size = 1;
    for (List<Object> dim : grid) {
      size *= dim.size();
      if (size > Long.MAX_VALUE / 2) return Long.MAX_VALUE;
    }
    return size;
  }

  private static void collect(AbstractExpression expr, List<Set<Object>> perDim) {
    if (expr instanceof AbstractExpression.Pred) {
      AbstractExpression.Pred p = (AbstractExpression.Pred) expr;
      if (p.dim >= 0 && p.dim < perDim.size()) {
        perDim.get(p.dim).add(p.value);
      }
      return;
    }
    if (expr instanceof AbstractExpression.And) {
      for (AbstractExpression c : ((AbstractExpression.And) expr).children) collect(c, perDim);
      return;
    }
    if (expr instanceof AbstractExpression.Or) {
      for (AbstractExpression c : ((AbstractExpression.Or) expr).children) collect(c, perDim);
      return;
    }
    // Unknown contributes nothing.
  }

  private static Object perturbDown(Object v) {
    if (v instanceof Long) return (Long) v - 1L;
    if (v instanceof Integer) return ((Integer) v) - 1;
    if (v instanceof BigDecimal) return ((BigDecimal) v).subtract(BigDecimal.ONE);
    if (v instanceof String) {
      String s = (String) v;
      if (s.isEmpty()) return null;
      char c = s.charAt(s.length() - 1);
      if (c == 0) return null;
      return s.substring(0, s.length() - 1) + (char) (c - 1);
    }
    return null;
  }

  private static Object perturbUp(Object v) {
    if (v instanceof Long) return (Long) v + 1L;
    if (v instanceof Integer) return ((Integer) v) + 1;
    if (v instanceof BigDecimal) return ((BigDecimal) v).add(BigDecimal.ONE);
    if (v instanceof String) {
      String s = (String) v;
      if (s.isEmpty()) return "a";
      char c = s.charAt(s.length() - 1);
      if (c >= Character.MAX_VALUE) return null;
      return s.substring(0, s.length() - 1) + (char) (c + 1);
    }
    return null;
  }

  /**
   * Compares arbitrary {@link Comparable}s, tolerating heterogeneous types within a dim
   * (e.g. a BigDecimal literal and an Integer perturbation).
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private enum AnyComparator implements java.util.Comparator<Object> {
    INSTANCE;

    @Override
    public int compare(Object a, Object b) {
      if (a == b) return 0;
      if (a == null) return -1;
      if (b == null) return 1;
      if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
        return ((Comparable) a).compareTo(b);
      }
      // Different types — compare by string form for determinism.
      return a.toString().compareTo(b.toString());
    }
  }
}
