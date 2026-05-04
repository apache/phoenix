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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The closure of {@link AbstractKeySpace} under OR — a list of N-dim boxes representing
 * the union of those boxes' rows. One box per non-mergeable OR branch.
 * <p>
 * The algebra:
 * <ul>
 * <li>{@link #and(AbstractKeySpaceList)} distributes AND over OR, then merges to a fixpoint
 * under {@link AbstractKeySpace#unionIfMergeable}.</li>
 * <li>{@link #or(AbstractKeySpaceList)} concatenates the two lists, then merges to a
 * fixpoint.</li>
 * </ul>
 * <p>
 * Two sentinel values:
 * <ul>
 * <li>{@link #unsatisfiable(int)} — empty list. No row satisfies it. Identity for OR.</li>
 * <li>{@link #everything(int)} — singleton {@link AbstractKeySpace#everything(int)}.
 * Every row satisfies it. Identity for AND.</li>
 * </ul>
 */
public final class AbstractKeySpaceList {

  private final List<AbstractKeySpace> spaces;
  private final int nDims;

  private AbstractKeySpaceList(int nDims, List<AbstractKeySpace> spaces) {
    this.nDims = nDims;
    this.spaces = Collections.unmodifiableList(spaces);
  }

  public static AbstractKeySpaceList unsatisfiable(int n) {
    return new AbstractKeySpaceList(n, Collections.<AbstractKeySpace>emptyList());
  }

  public static AbstractKeySpaceList everything(int n) {
    return new AbstractKeySpaceList(n, Collections.singletonList(AbstractKeySpace.everything(n)));
  }

  public static AbstractKeySpaceList of(int n, AbstractKeySpace... spaces) {
    List<AbstractKeySpace> list = new ArrayList<>(spaces.length);
    for (AbstractKeySpace s : spaces) {
      if (s.nDims() != n) throw new IllegalArgumentException("arity mismatch");
      if (!s.isEmpty()) list.add(s);
    }
    if (list.isEmpty()) return unsatisfiable(n);
    mergeToFixpoint(list);
    return new AbstractKeySpaceList(n, list);
  }

  public int nDims() { return nDims; }
  public int size() { return spaces.size(); }
  public List<AbstractKeySpace> spaces() { return spaces; }
  public boolean isUnsatisfiable() { return spaces.isEmpty(); }
  public boolean isEverything() {
    return spaces.size() == 1 && spaces.get(0).isEverything();
  }

  /**
   * AND distributes over OR: cross-product each pair of spaces, drop empties, merge to
   * fixpoint. Output size is bounded by {@code this.size() × other.size()}.
   */
  public AbstractKeySpaceList and(AbstractKeySpaceList other) {
    requireSameArity(other);
    if (this.isUnsatisfiable() || other.isUnsatisfiable()) return unsatisfiable(nDims);
    if (this.isEverything()) return other;
    if (other.isEverything()) return this;
    List<AbstractKeySpace> out = new ArrayList<>(this.spaces.size() * other.spaces.size());
    for (AbstractKeySpace a : this.spaces) {
      for (AbstractKeySpace b : other.spaces) {
        AbstractKeySpace c = a.and(b);
        if (!c.isEmpty()) out.add(c);
      }
    }
    if (out.isEmpty()) return unsatisfiable(nDims);
    mergeToFixpoint(out);
    return new AbstractKeySpaceList(nDims, out);
  }

  /**
   * OR concatenates and merges. {@link AbstractKeySpace#unionIfMergeable} handles the
   * two merge rules; spaces that can't be combined stay as separate entries.
   */
  public AbstractKeySpaceList or(AbstractKeySpaceList other) {
    requireSameArity(other);
    if (this.isUnsatisfiable()) return other;
    if (other.isUnsatisfiable()) return this;
    if (this.isEverything() || other.isEverything()) return everything(nDims);
    List<AbstractKeySpace> combined = new ArrayList<>(this.spaces.size() + other.spaces.size());
    combined.addAll(this.spaces);
    combined.addAll(other.spaces);
    mergeToFixpoint(combined);
    return new AbstractKeySpaceList(nDims, combined);
  }

  /**
   * Folds pairwise {@code unionIfMergeable} in-place until no merge succeeds. O(K²·N) per
   * round; rounds converge because each successful merge strictly reduces list size. No
   * fast paths, no hash buckets — this is the reference, clarity beats speed.
   */
  private static void mergeToFixpoint(List<AbstractKeySpace> list) {
    boolean progress = true;
    while (progress) {
      progress = false;
      outer:
      for (int i = 0; i < list.size(); i++) {
        for (int j = i + 1; j < list.size(); j++) {
          AbstractKeySpace merged = list.get(i).unionIfMergeable(list.get(j));
          if (merged != null) {
            list.set(i, merged);
            list.remove(j);
            progress = true;
            break outer;
          }
        }
      }
    }
  }

  /**
   * Does {@code row} satisfy this list? Used by correctness tests.
   */
  public boolean matches(List<Object> row) {
    for (AbstractKeySpace ks : spaces) {
      if (ks.matches(row)) return true;
    }
    return false;
  }

  /**
   * Drop the highest-indexed constrained dim across all spaces (replace with EVERYTHING
   * on every space, then re-merge). Implements the "drop trailing dimensions" rule for
   * cartesian-explosion mitigation. Returns {@link #everything(int)} when nothing is
   * left to drop.
   */
  public AbstractKeySpaceList dropTrailingDim() {
    if (spaces.isEmpty()) return this;
    int n = nDims;
    int highest = -1;
    for (int d = n - 1; d >= 0 && highest < 0; d--) {
      for (AbstractKeySpace ks : spaces) {
        if (!ks.get(d).isEverything()) {
          highest = d;
          break;
        }
      }
    }
    if (highest < 0) return everything(n);
    List<AbstractKeySpace> out = new ArrayList<>(spaces.size());
    for (AbstractKeySpace ks : spaces) {
      out.add(ks.withDimReplaced(highest, AbstractRange.everything()));
    }
    mergeToFixpoint(out);
    return new AbstractKeySpaceList(n, out);
  }

  private void requireSameArity(AbstractKeySpaceList other) {
    if (this.nDims != other.nDims) {
      throw new IllegalArgumentException("arity mismatch: " + nDims + " vs " + other.nDims);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AbstractKeySpaceList)) return false;
    AbstractKeySpaceList that = (AbstractKeySpaceList) o;
    return this.nDims == that.nDims && this.spaces.equals(that.spaces);
  }

  @Override
  public int hashCode() {
    return 31 * nDims + spaces.hashCode();
  }

  @Override
  public String toString() {
    if (spaces.isEmpty()) return "KSL[UNSAT n=" + nDims + "]";
    return "KSL" + spaces;
  }
}
