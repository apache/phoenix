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

import java.util.Objects;

/**
 * A 1-D interval over any {@link Comparable} type. Used by the reference implementation
 * (oracle) to model one dimension of an N-dimensional key space. Deliberately free of any
 * Phoenix dependency so the oracle can be exercised without an HBase cluster, a schema,
 * or a byte encoding.
 * <p>
 * Semantics are standard interval arithmetic:
 * <ul>
 * <li>{@code lo == null} means "unbounded below" (−∞).</li>
 * <li>{@code hi == null} means "unbounded above" (+∞).</li>
 * <li>{@code loInclusive} / {@code hiInclusive} carry inclusivity at each end.</li>
 * <li>An empty range means unsatisfiable — the singleton {@link #empty()}.</li>
 * <li>An everything range means unconstrained — {@code (−∞, +∞)}.</li>
 * </ul>
 * <p>
 * This class is the 1-D primitive for {@link AbstractKeySpace}'s per-dim slot. Operations
 * {@link #intersect}, {@link #union}, {@link #contains} are defined purely in terms of the
 * comparator, so a {@code Long} range and a {@code String} range can coexist in different
 * dims of the same key space.
 */
public final class AbstractRange<T extends Comparable<T>> {

  private final T lo;
  private final T hi;
  private final boolean loInclusive;
  private final boolean hiInclusive;
  private final boolean empty;

  private AbstractRange(T lo, T hi, boolean loInclusive, boolean hiInclusive, boolean empty) {
    this.lo = lo;
    this.hi = hi;
    this.loInclusive = loInclusive;
    this.hiInclusive = hiInclusive;
    this.empty = empty;
  }

  /** The unsatisfiable interval. */
  @SuppressWarnings("rawtypes")
  private static final AbstractRange EMPTY = new AbstractRange<>(null, null, false, false, true);

  /** The (−∞, +∞) interval — the AND identity and OR absorbing element. */
  @SuppressWarnings("rawtypes")
  private static final AbstractRange EVERYTHING = new AbstractRange<>(null, null, false, false,
    false);

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> AbstractRange<T> empty() {
    return (AbstractRange<T>) EMPTY;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> AbstractRange<T> everything() {
    return (AbstractRange<T>) EVERYTHING;
  }

  /** {@code [v, v]} — a point range. */
  public static <T extends Comparable<T>> AbstractRange<T> point(T v) {
    Objects.requireNonNull(v);
    return new AbstractRange<>(v, v, true, true, false);
  }

  /** {@code [v, +∞)}. */
  public static <T extends Comparable<T>> AbstractRange<T> atLeast(T v) {
    Objects.requireNonNull(v);
    return new AbstractRange<>(v, null, true, false, false);
  }

  /** {@code (v, +∞)}. */
  public static <T extends Comparable<T>> AbstractRange<T> greaterThan(T v) {
    Objects.requireNonNull(v);
    return new AbstractRange<>(v, null, false, false, false);
  }

  /** {@code (−∞, v]}. */
  public static <T extends Comparable<T>> AbstractRange<T> atMost(T v) {
    Objects.requireNonNull(v);
    return new AbstractRange<>(null, v, false, true, false);
  }

  /** {@code (−∞, v)}. */
  public static <T extends Comparable<T>> AbstractRange<T> lessThan(T v) {
    Objects.requireNonNull(v);
    return new AbstractRange<>(null, v, false, false, false);
  }

  /** General constructor. {@code null} bounds indicate unbounded ends. */
  public static <T extends Comparable<T>> AbstractRange<T> of(T lo, boolean loInclusive, T hi,
    boolean hiInclusive) {
    if (lo != null && hi != null) {
      int c = lo.compareTo(hi);
      if (c > 0) {
        return empty();
      }
      if (c == 0 && !(loInclusive && hiInclusive)) {
        return empty();
      }
    }
    return new AbstractRange<>(lo, hi, loInclusive, hiInclusive, false);
  }

  public boolean isEmpty() {
    return empty;
  }

  public boolean isEverything() {
    return !empty && lo == null && hi == null;
  }

  public boolean isSingleKey() {
    return !empty && lo != null && hi != null && loInclusive && hiInclusive && lo.equals(hi);
  }

  public T lo() {
    return lo;
  }

  public T hi() {
    return hi;
  }

  public boolean loInclusive() {
    return loInclusive;
  }

  public boolean hiInclusive() {
    return hiInclusive;
  }

  public boolean loUnbounded() {
    return lo == null;
  }

  public boolean hiUnbounded() {
    return hi == null;
  }

  /** Does {@code v} satisfy this range? */
  public boolean contains(T v) {
    if (empty) return false;
    if (lo != null) {
      int c = v.compareTo(lo);
      if (c < 0 || (c == 0 && !loInclusive)) return false;
    }
    if (hi != null) {
      int c = v.compareTo(hi);
      if (c > 0 || (c == 0 && !hiInclusive)) return false;
    }
    return true;
  }

  /**
   * Standard interval intersection. Returns {@link #empty()} when the intervals don't
   * overlap. Correctness here is by direct algebra — {@code max(lo)} and {@code min(hi)}
   * with careful inclusivity at each endpoint.
   */
  public AbstractRange<T> intersect(AbstractRange<T> other) {
    if (this.empty || other.empty) return empty();
    if (this.isEverything()) return other;
    if (other.isEverything()) return this;

    T newLo;
    boolean newLoInc;
    if (this.lo == null) {
      newLo = other.lo;
      newLoInc = other.loInclusive;
    } else if (other.lo == null) {
      newLo = this.lo;
      newLoInc = this.loInclusive;
    } else {
      int c = this.lo.compareTo(other.lo);
      if (c > 0) {
        newLo = this.lo;
        newLoInc = this.loInclusive;
      } else if (c < 0) {
        newLo = other.lo;
        newLoInc = other.loInclusive;
      } else {
        newLo = this.lo;
        newLoInc = this.loInclusive && other.loInclusive;
      }
    }

    T newHi;
    boolean newHiInc;
    if (this.hi == null) {
      newHi = other.hi;
      newHiInc = other.hiInclusive;
    } else if (other.hi == null) {
      newHi = this.hi;
      newHiInc = this.hiInclusive;
    } else {
      int c = this.hi.compareTo(other.hi);
      if (c < 0) {
        newHi = this.hi;
        newHiInc = this.hiInclusive;
      } else if (c > 0) {
        newHi = other.hi;
        newHiInc = other.hiInclusive;
      } else {
        newHi = this.hi;
        newHiInc = this.hiInclusive && other.hiInclusive;
      }
    }

    if (newLo != null && newHi != null) {
      int c = newLo.compareTo(newHi);
      if (c > 0) return empty();
      if (c == 0 && !(newLoInc && newHiInc)) return empty();
    }
    return new AbstractRange<>(newLo, newHi, newLoInc, newHiInc, false);
  }

  /**
   * Union when the two intervals overlap or touch (adjacent at the shared endpoint with one
   * side inclusive). If they are disjoint (non-touching), returns {@code null} so the caller
   * knows the union is not a single interval and must be kept as two separate entries.
   * <p>
   * OR rule 2 requires non-disjoint ranges on the merging dim; this method encodes that
   * as "single-interval union exists ⟺ non-disjoint-or-adjacent".
   */
  public AbstractRange<T> union(AbstractRange<T> other) {
    if (this.empty) return other;
    if (other.empty) return this;
    if (this.isEverything() || other.isEverything()) return everything();

    if (!overlapsOrTouches(this, other)) return null;

    T newLo;
    boolean newLoInc;
    if (this.lo == null || other.lo == null) {
      newLo = null;
      newLoInc = false;
    } else {
      int c = this.lo.compareTo(other.lo);
      if (c < 0) {
        newLo = this.lo;
        newLoInc = this.loInclusive;
      } else if (c > 0) {
        newLo = other.lo;
        newLoInc = other.loInclusive;
      } else {
        newLo = this.lo;
        newLoInc = this.loInclusive || other.loInclusive;
      }
    }

    T newHi;
    boolean newHiInc;
    if (this.hi == null || other.hi == null) {
      newHi = null;
      newHiInc = false;
    } else {
      int c = this.hi.compareTo(other.hi);
      if (c > 0) {
        newHi = this.hi;
        newHiInc = this.hiInclusive;
      } else if (c < 0) {
        newHi = other.hi;
        newHiInc = other.hiInclusive;
      } else {
        newHi = this.hi;
        newHiInc = this.hiInclusive || other.hiInclusive;
      }
    }
    return new AbstractRange<>(newLo, newHi, newLoInc, newHiInc, false);
  }

  /** {@code this} ⊇ {@code other}. */
  public boolean contains(AbstractRange<T> other) {
    if (other.empty) return true;
    if (this.empty) return false;
    if (this.isEverything()) return true;
    // Lower bound of {@code this} must be at-or-below {@code other}'s.
    if (this.lo != null) {
      if (other.lo == null) return false;
      int c = this.lo.compareTo(other.lo);
      if (c > 0) return false;
      if (c == 0 && !this.loInclusive && other.loInclusive) return false;
    }
    // Upper bound of {@code this} must be at-or-above {@code other}'s.
    if (this.hi != null) {
      if (other.hi == null) return false;
      int c = this.hi.compareTo(other.hi);
      if (c < 0) return false;
      if (c == 0 && !this.hiInclusive && other.hiInclusive) return false;
    }
    return true;
  }

  /**
   * True iff the two ranges overlap OR are adjacent (share an endpoint with one inclusive,
   * the other not inclusive — so the shared point is covered by exactly one of them). Used
   * to decide whether {@link #union} can produce a single interval.
   */
  private static <T extends Comparable<T>> boolean overlapsOrTouches(AbstractRange<T> a,
    AbstractRange<T> b) {
    if (a.empty || b.empty) return false;
    // Disjoint: a.hi < b.lo, or a.hi == b.lo with both sides exclusive.
    if (a.hi != null && b.lo != null) {
      int c = a.hi.compareTo(b.lo);
      if (c < 0) return false;
      if (c == 0 && !a.hiInclusive && !b.loInclusive) return false;
    }
    if (b.hi != null && a.lo != null) {
      int c = b.hi.compareTo(a.lo);
      if (c < 0) return false;
      if (c == 0 && !b.hiInclusive && !a.loInclusive) return false;
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractRange)) return false;
    AbstractRange<?> that = (AbstractRange<?>) o;
    if (this.empty || that.empty) return this.empty == that.empty;
    return this.loInclusive == that.loInclusive && this.hiInclusive == that.hiInclusive
      && Objects.equals(this.lo, that.lo) && Objects.equals(this.hi, that.hi);
  }

  @Override
  public int hashCode() {
    if (empty) return 0;
    return Objects.hash(lo, hi, loInclusive, hiInclusive);
  }

  @Override
  public String toString() {
    if (empty) return "∅";
    if (isEverything()) return "(-∞, +∞)";
    StringBuilder sb = new StringBuilder();
    sb.append(loInclusive ? '[' : '(');
    sb.append(lo == null ? "-∞" : lo);
    sb.append(", ");
    sb.append(hi == null ? "+∞" : hi);
    sb.append(hiInclusive ? ']' : ')');
    return sb.toString();
  }
}
