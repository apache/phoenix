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

import java.util.Arrays;
import java.util.Optional;

import org.apache.phoenix.query.KeyRange;

/**
 * An N-dimensional key space over a table's primary key columns. Each dimension is a
 * {@link KeyRange} over the encoded byte representation of a single PK column. An
 * expression node is modeled as a list of {@code KeySpace} instances; see
 * {@link KeySpaceList} for the list-level algebra.
 * <p>
 * Instances are immutable. {@link #and(KeySpace)} is the per-dimension intersection;
 * {@link #unionIfMergeable(KeySpace)} returns the union when either (a) one space contains
 * the other or (b) the two spaces agree on all but one dimension and the differing dim's
 * ranges are non-disjoint.
 * <p>
 * A single-value predicate like {@code PK2 >= 3} on a 3-PK table is represented as
 * {@code [(*,*), [3,*), (*,*)]} — a singleton {@link KeySpaceList} containing a single
 * {@code KeySpace} where every dim not mentioned in the predicate holds
 * {@link KeyRange#EVERYTHING_RANGE}. RVC inequalities are pre-normalized by
 * {@link ExpressionNormalizer} into lexicographic AND/OR of scalar comparisons so that
 * per-dim intersection composes correctly with every other predicate; this class therefore
 * never needs to model compound-byte concatenation directly.
 */
public final class KeySpace {

  private final KeyRange[] dims;
  private final boolean empty;

  private KeySpace(KeyRange[] dims, boolean empty) {
    this.dims = dims;
    this.empty = empty;
  }

  public static KeySpace everything(int n) {
    KeyRange[] dims = new KeyRange[n];
    Arrays.fill(dims, KeyRange.EVERYTHING_RANGE);
    return new KeySpace(dims, false);
  }

  public static KeySpace empty(int n) {
    KeyRange[] dims = new KeyRange[n];
    Arrays.fill(dims, KeyRange.EMPTY_RANGE);
    return new KeySpace(dims, true);
  }

  public static KeySpace single(int dim, KeyRange r, int n) {
    if (r == KeyRange.EMPTY_RANGE) {
      return empty(n);
    }
    KeyRange[] dims = new KeyRange[n];
    Arrays.fill(dims, KeyRange.EVERYTHING_RANGE);
    dims[dim] = r;
    return new KeySpace(dims, false);
  }

  public static KeySpace of(KeyRange[] dims) {
    boolean empty = false;
    for (KeyRange r : dims) {
      if (r == KeyRange.EMPTY_RANGE) {
        empty = true;
        break;
      }
    }
    return new KeySpace(dims.clone(), empty);
  }

  public int nDims() {
    return dims.length;
  }

  public KeyRange get(int dim) {
    return dims[dim];
  }

  /**
   * Returns a new {@link KeySpace} identical to this one except with dim {@code dim}
   * replaced by {@code r}. Used by {@link KeySpaceList}'s widening path to drop a
   * trailing dim (by replacing it with {@link KeyRange#EVERYTHING_RANGE}). Allocates a
   * fresh dims array; original is unchanged.
   */
  public KeySpace withDimReplaced(int dim, KeyRange r) {
    if (dims[dim].equals(r)) {
      return this;
    }
    KeyRange[] newDims = dims.clone();
    newDims[dim] = r;
    if (r == KeyRange.EMPTY_RANGE) {
      return empty(dims.length);
    }
    return new KeySpace(newDims, false);
  }

  public boolean isEmpty() {
    return empty;
  }

  public boolean isEverything() {
    if (empty) {
      return false;
    }
    for (KeyRange r : dims) {
      if (r != KeyRange.EVERYTHING_RANGE) {
        return false;
      }
    }
    return true;
  }

  /**
   * Per-dimension intersection. If any dim collapses to {@link KeyRange#EMPTY_RANGE}, the
   * result is {@link #empty(int)}.
   */
  public KeySpace and(KeySpace other) {
    requireSameArity(other);
    if (this.empty || other.empty) {
      return empty(dims.length);
    }
    KeyRange[] newDims = new KeyRange[dims.length];
    for (int i = 0; i < dims.length; i++) {
      KeyRange a = this.dims[i];
      KeyRange b = other.dims[i];
      KeyRange inter = intersectRange(a, b);
      if (inter == KeyRange.EMPTY_RANGE) {
        return empty(dims.length);
      }
      newDims[i] = inter;
    }
    return new KeySpace(newDims, false);
  }

  /**
   * Per-dim intersection that special-cases {@link KeyRange#EVERYTHING_RANGE} against
   * {@link KeyRange#IS_NULL_RANGE} / {@link KeyRange#IS_NOT_NULL_RANGE}. Plain
   * {@link KeyRange#intersect} treats EVERYTHING ∩ IS_NULL as EMPTY because IS_NULL uses
   * an empty-byte-array sentinel that coincides with the EVERYTHING representation.
   */
  private static KeyRange intersectRange(KeyRange a, KeyRange b) {
    if (a == KeyRange.EVERYTHING_RANGE) {
      return b;
    }
    if (b == KeyRange.EVERYTHING_RANGE) {
      return a;
    }
    return a.intersect(b);
  }

  /**
   * Returns the union of {@code this} and {@code other} as a single {@code KeySpace} when
   * one of the two merge rules applies; otherwise {@link Optional#empty()}.
   * <ul>
   * <li>Rule 1 (containment): one space is fully contained in the other; return the larger.</li>
   * <li>Rule 2 (adjacent boxes): agreeing on all-but-one dim and the remaining dim's ranges
   * overlap or are adjacent; return the space with the merged dim's range.</li>
   * </ul>
   */
  public Optional<KeySpace> unionIfMergeable(KeySpace other) {
    requireSameArity(other);
    if (this.empty) {
      return Optional.of(other);
    }
    if (other.empty) {
      return Optional.of(this);
    }
    if (this.equals(other)) {
      return Optional.of(this);
    }
    if (contains(other)) {
      return Optional.of(this);
    }
    if (other.contains(this)) {
      return Optional.of(other);
    }
    int diffDim = -1;
    for (int i = 0; i < dims.length; i++) {
      if (!this.dims[i].equals(other.dims[i])) {
        if (diffDim != -1) {
          return Optional.empty();
        }
        diffDim = i;
      }
    }
    if (diffDim == -1) {
      return Optional.of(this);
    }
    KeyRange a = this.dims[diffDim];
    KeyRange b = other.dims[diffDim];
    // Two distinct single-key points are disjoint and NOT adjacent (they're different
    // values). KeyRange.intersect has a bug for inverted (DESC) singleton pairs where
    // the intersection is computed as a non-empty backward range rather than
    // EMPTY_RANGE, so the check below would incorrectly fall through to union. Detect
    // this shape explicitly.
    if (a.isSingleKey() && b.isSingleKey()
      && !java.util.Arrays.equals(a.getLowerRange(), b.getLowerRange())) {
      return Optional.empty();
    }
    if (a.intersect(b) == KeyRange.EMPTY_RANGE && !isAdjacent(a, b)) {
      return Optional.empty();
    }
    KeyRange[] newDims = dims.clone();
    newDims[diffDim] = a.union(b);
    return Optional.of(new KeySpace(newDims, false));
  }

  /**
   * Two 1-D ranges are adjacent when the upper bound of one equals the lower bound of the
   * other and exactly one side is inclusive (so together they cover the shared endpoint
   * exactly once).
   */
  private static boolean isAdjacent(KeyRange a, KeyRange b) {
    return adjacentOneWay(a, b) || adjacentOneWay(b, a);
  }

  private static boolean adjacentOneWay(KeyRange a, KeyRange b) {
    if (a.upperUnbound() || b.lowerUnbound()) {
      return false;
    }
    if (!java.util.Arrays.equals(a.getUpperRange(), b.getLowerRange())) {
      return false;
    }
    return a.isUpperInclusive() != b.isLowerInclusive();
  }

  /**
   * {@code this} contains {@code other} iff every dim of {@code this} contains the dim of
   * {@code other}.
   */
  public boolean contains(KeySpace other) {
    requireSameArity(other);
    if (other.empty) {
      return true;
    }
    if (this.empty) {
      return false;
    }
    for (int i = 0; i < dims.length; i++) {
      // Use the EVERYTHING-aware intersect: raw KeyRange.intersect() collapses
      // EVERYTHING ∩ IS_NULL to EMPTY because IS_NULL's empty-byte sentinel and
      // EVERYTHING's UNBOUND both read as empty byte arrays. Without this,
      // [EVERYTHING,...] would falsely report it does not contain [IS_NULL,...],
      // breaking OR-merge containment for IS_NULL predicates.
      KeyRange inter = intersectRange(this.dims[i], other.dims[i]);
      if (!inter.equals(other.dims[i])) {
        return false;
      }
    }
    return true;
  }

  private void requireSameArity(KeySpace other) {
    if (other.dims.length != this.dims.length) {
      throw new IllegalArgumentException(
        "KeySpace arity mismatch: " + this.dims.length + " vs " + other.dims.length);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeySpace)) {
      return false;
    }
    KeySpace that = (KeySpace) o;
    if (this.empty != that.empty) {
      return false;
    }
    if (this.empty) {
      return this.dims.length == that.dims.length;
    }
    if (this.dims.length != that.dims.length) {
      return false;
    }
    for (int i = 0; i < dims.length; i++) {
      if (!this.dims[i].equals(that.dims[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (empty) {
      return 31 * dims.length;
    }
    int h = 1;
    for (KeyRange r : dims) {
      h = 31 * h + r.hashCode();
    }
    return h;
  }

  @Override
  public String toString() {
    if (empty) {
      return "KeySpace[EMPTY, n=" + dims.length + "]";
    }
    StringBuilder sb = new StringBuilder("KeySpace[");
    for (int i = 0; i < dims.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(dims[i]);
    }
    return sb.append(']').toString();
  }

  /**
   * A hashable representative of {@code this}'s dim tuple with position {@code wildcard}
   * excluded. Two spaces share a signature iff they agree on every dim except possibly
   * {@code wildcard} — the exact precondition for rule 2 of {@link #unionIfMergeable}.
   * Used by {@link KeySpaceList#mergeToFixpoint} to group mergeable spaces in O(K) via a
   * hash map, avoiding the naive O(K²) pair scan.
   */
  public Signature signatureExcluding(int wildcard) {
    return new Signature(dims, wildcard, empty);
  }

  /** Opaque hashable/comparable key. Equal signatures indicate potential mergeability. */
  public static final class Signature {
    private final KeyRange[] dims;
    private final int wildcard;
    private final boolean empty;
    private final int hash;

    Signature(KeyRange[] dims, int wildcard, boolean empty) {
      this.dims = dims;
      this.wildcard = wildcard;
      this.empty = empty;
      int h = empty ? 1 : 0;
      for (int i = 0; i < dims.length; i++) {
        if (i == wildcard) {
          continue;
        }
        h = 31 * h + dims[i].hashCode();
      }
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Signature)) {
        return false;
      }
      Signature that = (Signature) o;
      if (that.hash != this.hash || that.wildcard != this.wildcard
        || that.empty != this.empty || that.dims.length != this.dims.length) {
        return false;
      }
      for (int i = 0; i < dims.length; i++) {
        if (i == wildcard) {
          continue;
        }
        if (!this.dims[i].equals(that.dims[i])) {
          return false;
        }
      }
      return true;
    }
  }
}
