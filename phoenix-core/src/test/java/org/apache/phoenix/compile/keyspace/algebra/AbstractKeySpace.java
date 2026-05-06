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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An N-dim box: one {@link AbstractRange} per primary-key dimension. The key-space
 * primitive of the V2 optimizer. Dimensions can hold ranges over different value types
 * (e.g. dim 0 is {@code String}, dim 1 is {@code Long}) — the dim index plus the per-range
 * {@code <T>} carries the type.
 * <p>
 * This class intentionally uses raw {@code AbstractRange} entries ({@code AbstractRange<?>})
 * because Java's type system cannot express a heterogeneous tuple of typed ranges without
 * per-test ceremony. The oracle's correctness does not depend on type parity — each range
 * internally uses {@link Comparable#compareTo} on its own typed bounds, so mixing types
 * across dims is safe as long as no operation compares ranges of different dims to each
 * other (which no AND/OR rule does — per-dim intersection/union stays within one dim).
 */
public final class AbstractKeySpace {

  private final AbstractRange<?>[] dims;
  private final boolean empty;

  private AbstractKeySpace(AbstractRange<?>[] dims, boolean empty) {
    this.dims = dims;
    this.empty = empty;
  }

  /** All-EVERYTHING — the AND identity. */
  public static AbstractKeySpace everything(int n) {
    AbstractRange<?>[] dims = new AbstractRange<?>[n];
    Arrays.fill(dims, AbstractRange.everything());
    return new AbstractKeySpace(dims, false);
  }

  /** All-EMPTY — unsatisfiable on every dim. */
  public static AbstractKeySpace empty(int n) {
    AbstractRange<?>[] dims = new AbstractRange<?>[n];
    Arrays.fill(dims, AbstractRange.empty());
    return new AbstractKeySpace(dims, true);
  }

  /** A KeySpace with EVERYTHING on every dim except {@code dim}, which carries {@code r}. */
  public static AbstractKeySpace single(int dim, AbstractRange<?> r, int n) {
    if (r.isEmpty()) return empty(n);
    AbstractRange<?>[] dims = new AbstractRange<?>[n];
    Arrays.fill(dims, AbstractRange.everything());
    dims[dim] = r;
    return new AbstractKeySpace(dims, false);
  }

  /** Construct from an explicit per-dim array. */
  public static AbstractKeySpace of(AbstractRange<?>... dims) {
    AbstractRange<?>[] copy = dims.clone();
    for (AbstractRange<?> r : copy) {
      if (r.isEmpty()) return empty(copy.length);
    }
    return new AbstractKeySpace(copy, false);
  }

  public int nDims() {
    return dims.length;
  }

  public AbstractRange<?> get(int dim) {
    return dims[dim];
  }

  public boolean isEmpty() {
    return empty;
  }

  public boolean isEverything() {
    if (empty) return false;
    for (AbstractRange<?> r : dims) {
      if (!r.isEverything()) return false;
    }
    return true;
  }

  /**
   * Per-dim intersection. The AND operation on key spaces: the intersection of two key
   * spaces is the intersection of each corresponding pair of dim ranges. Any dim collapsing
   * to empty makes the whole space empty.
   */
  public AbstractKeySpace and(AbstractKeySpace other) {
    requireSameArity(other);
    if (this.empty || other.empty) return empty(dims.length);
    AbstractRange<?>[] out = new AbstractRange<?>[dims.length];
    for (int i = 0; i < dims.length; i++) {
      AbstractRange<?> r = intersectAny(this.dims[i], other.dims[i]);
      if (r.isEmpty()) return empty(dims.length);
      out[i] = r;
    }
    return new AbstractKeySpace(out, false);
  }

  /**
   * Attempts the OR merge rules:
   * <ul>
   * <li>Rule 1: one space contains the other → return the larger.</li>
   * <li>Rule 2: agreeing on N−1 dims and the differing dim's ranges overlap or are
   * adjacent → return the space with the merged dim's range.</li>
   * </ul>
   * If neither rule applies, returns {@code null} so the caller must keep both spaces.
   */
  public AbstractKeySpace unionIfMergeable(AbstractKeySpace other) {
    requireSameArity(other);
    if (this.empty) return other;
    if (other.empty) return this;
    if (this.equals(other)) return this;
    if (this.contains(other)) return this;
    if (other.contains(this)) return other;

    int diffDim = -1;
    for (int i = 0; i < dims.length; i++) {
      if (!this.dims[i].equals(other.dims[i])) {
        if (diffDim != -1) return null; // disagree on more than one dim
        diffDim = i;
      }
    }
    if (diffDim == -1) return this; // equal (shouldn't reach here given earlier check)

    AbstractRange<?> merged = unionAny(this.dims[diffDim], other.dims[diffDim]);
    if (merged == null) return null; // disjoint on the differing dim
    AbstractRange<?>[] out = dims.clone();
    out[diffDim] = merged;
    return new AbstractKeySpace(out, false);
  }

  /** {@code this} contains {@code other} iff every dim of {@code this} contains the dim of {@code other}. */
  public boolean contains(AbstractKeySpace other) {
    requireSameArity(other);
    if (other.empty) return true;
    if (this.empty) return false;
    for (int i = 0; i < dims.length; i++) {
      if (!containsAny(this.dims[i], other.dims[i])) return false;
    }
    return true;
  }

  /**
   * Does the concrete tuple {@code row} satisfy this key space? Used by correctness tests
   * to verify that the emitted ranges contain all rows matching the original expression.
   */
  public boolean matches(List<Object> row) {
    if (empty) return false;
    if (row.size() != dims.length) {
      throw new IllegalArgumentException(
        "row arity " + row.size() + " != nDims " + dims.length);
    }
    for (int i = 0; i < dims.length; i++) {
      if (!containsValueAny(dims[i], row.get(i))) return false;
    }
    return true;
  }

  /** Returns a fresh KeySpace with dim {@code d} replaced by {@code r}. */
  public AbstractKeySpace withDimReplaced(int d, AbstractRange<?> r) {
    if (this.dims[d].equals(r)) return this;
    if (r.isEmpty()) return empty(dims.length);
    AbstractRange<?>[] out = dims.clone();
    out[d] = r;
    return new AbstractKeySpace(out, false);
  }

  /** First dim at or after {@code from} with a non-EVERYTHING range, or {@code -1}. */
  public int firstConstrainedDim(int from) {
    for (int d = from; d < dims.length; d++) {
      if (!dims[d].isEverything()) return d;
    }
    return -1;
  }

  /**
   * Length of the leading non-EVERYTHING run starting at {@code from}. The productive
   * prefix length — dims past the first EVERYTHING are ignored when emitting scan ranges.
   */
  public int productiveLen(int from) {
    int d = from;
    while (d < dims.length && !dims[d].isEverything()) d++;
    return d - from;
  }

  // ------- private helpers for raw-typed range operations -------
  //
  // These cast to AbstractRange<Comparable>. They are safe because any two ranges we pass
  // here come from the SAME dim position of SAME-arity spaces, and the caller (AND / OR)
  // never mixes ranges across dims. The @SuppressWarnings is bounded to these three helpers.

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractRange<?> intersectAny(AbstractRange<?> a, AbstractRange<?> b) {
    return ((AbstractRange) a).intersect((AbstractRange) b);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static AbstractRange<?> unionAny(AbstractRange<?> a, AbstractRange<?> b) {
    return ((AbstractRange) a).union((AbstractRange) b);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static boolean containsAny(AbstractRange<?> a, AbstractRange<?> b) {
    return ((AbstractRange) a).contains((AbstractRange) b);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static boolean containsValueAny(AbstractRange<?> r, Object v) {
    return ((AbstractRange) r).contains((Comparable) v);
  }

  private void requireSameArity(AbstractKeySpace other) {
    if (this.dims.length != other.dims.length) {
      throw new IllegalArgumentException(
        "arity mismatch: " + this.dims.length + " vs " + other.dims.length);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractKeySpace)) return false;
    AbstractKeySpace that = (AbstractKeySpace) o;
    if (this.empty != that.empty) return false;
    if (this.empty) return this.dims.length == that.dims.length;
    return Arrays.equals(this.dims, that.dims);
  }

  @Override
  public int hashCode() {
    return empty ? -1 : Arrays.hashCode(dims);
  }

  @Override
  public String toString() {
    if (empty) return "KS[EMPTY n=" + dims.length + "]";
    List<String> parts = new ArrayList<>(dims.length);
    for (AbstractRange<?> r : dims) parts.add(r.toString());
    return "KS" + parts;
  }
}
