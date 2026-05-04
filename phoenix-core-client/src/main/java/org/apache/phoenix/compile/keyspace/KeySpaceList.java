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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * An immutable list of {@link KeySpace} instances representing one expression node's
 * contribution to the WHERE optimizer. The list is the closure of {@link KeySpace} under
 * OR: a single {@code KeySpace} is not sufficient because {@code OR} of two non-mergeable
 * spaces produces two spaces.
 * <p>
 * The algebra is:
 * <ul>
 * <li>{@link #and(KeySpaceList)} distributes AND over OR, then merges to fixpoint.</li>
 * <li>{@link #or(KeySpaceList)} concatenates and merges to fixpoint.</li>
 * </ul>
 * An empty list is unsatisfiable (the expression cannot be true). The "everything" list
 * is the singleton containing {@link KeySpace#everything(int)}.
 */
public final class KeySpaceList {

  private final List<KeySpace> spaces;
  private final int nDims;

  private KeySpaceList(int nDims, List<KeySpace> spaces) {
    this.nDims = nDims;
    this.spaces = Collections.unmodifiableList(spaces);
  }

  public static KeySpaceList unsatisfiable(int nDims) {
    return new KeySpaceList(nDims, Collections.<KeySpace>emptyList());
  }

  public static KeySpaceList everything(int nDims) {
    return new KeySpaceList(nDims,
      Collections.singletonList(KeySpace.everything(nDims)));
  }

  public static KeySpaceList of(KeySpace... spaces) {
    if (spaces.length == 0) {
      throw new IllegalArgumentException("Use unsatisfiable(n) for empty lists");
    }
    int nDims = spaces[0].nDims();
    List<KeySpace> list = new ArrayList<>(spaces.length);
    for (KeySpace s : spaces) {
      if (s.nDims() != nDims) {
        throw new IllegalArgumentException("arity mismatch");
      }
      if (!s.isEmpty()) {
        list.add(s);
      }
    }
    return fromNormalized(nDims, list);
  }

  public static KeySpaceList of(List<KeySpace> spaces, int nDims) {
    List<KeySpace> filtered = new ArrayList<>(spaces.size());
    for (KeySpace s : spaces) {
      if (s.nDims() != nDims) {
        throw new IllegalArgumentException("arity mismatch");
      }
      if (!s.isEmpty()) {
        filtered.add(s);
      }
    }
    return fromNormalized(nDims, filtered);
  }

  private static KeySpaceList fromNormalized(int nDims, List<KeySpace> filtered) {
    if (filtered.isEmpty()) {
      return unsatisfiable(nDims);
    }
    mergeToFixpoint(filtered);
    KeySpaceList out = new KeySpaceList(nDims, filtered);
    // Enforce the cartesian bound uniformly at every list-construction point. AND's
    // cross-product path already pre-widens before calling us (so this rarely fires
    // from AND), but OR/IN paths route all unioned branches through here, and a single
    // post-merge list could still exceed the bound when many branches didn't merge
    // (e.g., 100k distinct point keys on one dim). Widening drops trailing dims until
    // the size fits — same rule as AND — so every operation has a consistent upper
    // bound on memory and downstream work.
    if (out.spaces.size() > CARTESIAN_BOUND) {
      return widenToBudget(out, CARTESIAN_BOUND);
    }
    return out;
  }

  public int nDims() {
    return nDims;
  }

  public int size() {
    return spaces.size();
  }

  public List<KeySpace> spaces() {
    return spaces;
  }

  public boolean isUnsatisfiable() {
    return spaces.isEmpty();
  }

  public boolean isEverything() {
    return spaces.size() == 1 && spaces.get(0).isEverything();
  }

  /**
   * Upper bound on the number of spaces a {@link KeySpaceList} may hold. Every operation
   * that could produce a list above this bound — AND cross-products, OR concatenations —
   * applies the widening rule in {@link #enforceCartesianBound} instead of enumerating
   * the full product.
   * <p>
   * Set to 65,536: well above the scan-range bound (50,000) so normal queries aren't
   * affected, but low enough that even a double-exceeded product (4 × bound) still
   * computes fast. Enforced uniformly via {@link #fromNormalized}, so no code path can
   * bypass it.
   */
  private static final int CARTESIAN_BOUND = 65_536;

  /**
   * AND distributes over OR: for each pair {@code (a ∈ this, b ∈ other)} compute
   * {@code a.and(b)}, drop empties, then normalize. The output size is bounded by
   * {@code this.size() × other.size()}, but — critically — we don't enumerate that
   * product when it would exceed {@link #CARTESIAN_BOUND}. Instead, we apply the design's
   * "drop trailing dims" widening to the larger side until its size falls to
   * {@code ceil(bound / smaller.size())}, then do the bounded cross-product.
   * <p>
   * Widening only drops information (every key the original matched is still matched by
   * the widened list). The residual filter enforces the dropped predicates at scan time,
   * so correctness is preserved. Scan narrowing on the kept dims is unchanged.
   */
  public KeySpaceList and(KeySpaceList other) {
    requireSameArity(other);
    if (this.isUnsatisfiable() || other.isUnsatisfiable()) {
      return unsatisfiable(nDims);
    }
    if (this.isEverything()) {
      return other;
    }
    if (other.isEverything()) {
      return this;
    }
    KeySpaceList left = this;
    KeySpaceList right = other;
    long productSize = (long) left.spaces.size() * (long) right.spaces.size();
    if (productSize > CARTESIAN_BOUND) {
      // Choose the smaller side as the cap denominator. Widen the larger side down to
      // ceil(bound / smaller.size()); that guarantees the post-widen product fits.
      if (left.spaces.size() > right.spaces.size()) {
        KeySpaceList tmp = left; left = right; right = tmp;
      }
      int budget = Math.max(1, CARTESIAN_BOUND / Math.max(1, left.spaces.size()));
      right = widenToBudget(right, budget);
      productSize = (long) left.spaces.size() * (long) right.spaces.size();
    }
    List<KeySpace> result = new ArrayList<>((int) Math.min(productSize, CARTESIAN_BOUND));
    for (KeySpace a : left.spaces) {
      for (KeySpace b : right.spaces) {
        KeySpace c = a.and(b);
        if (!c.isEmpty()) {
          result.add(c);
        }
      }
    }
    return fromNormalized(nDims, result);
  }

  /**
   * Widens a list down to at most {@code budget} spaces by dropping trailing dims (design
   * rule "drop trailing dims to prevent range explosion"). Each drop replaces one dim
   * with {@link KeyRange#EVERYTHING_RANGE} in every space, then re-normalizes —
   * duplicates collapse via the merge fixpoint. Repeats until size ≤ budget or there's
   * nothing left to drop; in the worst case returns a single all-EVERYTHING KeySpace.
   * <p>
   * The choice of *which* trailing dim to drop matters for residual-filter correctness.
   * We drop the highest-indexed dim that is constrained in at least one space — the
   * leading dims do the bulk of the scan narrowing and should be preserved. O(K · N · D)
   * where D is the number of drops performed (at most N), so overall O(K · N²).
   */
  private static KeySpaceList widenToBudget(KeySpaceList list, int budget) {
    int n = list.nDims;
    List<KeySpace> current = new ArrayList<>(list.spaces);
    while (current.size() > budget) {
      int trailing = highestConstrainedDim(current);
      if (trailing < 0) {
        return everything(n);
      }
      // Drop dim `trailing` from every space, then merge duplicates. We call
      // mergeToFixpoint directly (not fromNormalized) to avoid re-triggering the
      // bound-enforcement recursion — we're in the middle of enforcing it.
      List<KeySpace> dropped = new ArrayList<>(current.size());
      for (KeySpace ks : current) {
        dropped.add(ks.withDimReplaced(trailing, org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE));
      }
      mergeToFixpoint(dropped);
      if (dropped.size() >= current.size()) {
        // No progress — bail to avoid infinite loop. Conservative but safe.
        return everything(n);
      }
      current = dropped;
    }
    mergeToFixpoint(current);
    return new KeySpaceList(n, current);
  }

  /**
   * Returns the highest dim index that is constrained (not EVERYTHING) in at least one
   * space of the list, or {@code -1} if every space is all-EVERYTHING. Used to pick the
   * next trailing dim to drop during widening.
   */
  private static int highestConstrainedDim(List<KeySpace> list) {
    if (list.isEmpty()) return -1;
    int n = list.get(0).nDims();
    for (int d = n - 1; d >= 0; d--) {
      for (KeySpace ks : list) {
        if (ks.get(d) != org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE) {
          return d;
        }
      }
    }
    return -1;
  }

  /**
   * OR is the union of the two lists with pairwise merges folded to a fixpoint.
   */
  public KeySpaceList or(KeySpaceList other) {
    requireSameArity(other);
    if (this.isUnsatisfiable()) {
      return other;
    }
    if (other.isUnsatisfiable()) {
      return this;
    }
    if (this.isEverything() || other.isEverything()) {
      return everything(nDims);
    }
    List<KeySpace> combined = new ArrayList<>(this.spaces.size() + other.spaces.size());
    combined.addAll(this.spaces);
    combined.addAll(other.spaces);
    return fromNormalized(nDims, combined);
  }

  /**
   * Bulk-OR variant for large OR nodes. Collects every branch's spaces into a single list,
   * then runs the merge-fixpoint once. Equivalent to folding {@link #or(KeySpaceList)}
   * left-to-right, but avoids the quadratic fold cost of re-merging the accumulator on
   * every step — with K branches the left-fold runs K mergeToFixpoint passes over lists
   * of growing size, whereas this runs exactly one pass over the concatenated list.
   * <p>
   * Used by {@link KeySpaceExpressionVisitor#visitLeave(OrExpression, List)} for OR nodes
   * with more than a handful of children; benchmark shows ~45× improvement at K=500.
   */
  public static KeySpaceList orAll(int nDims, List<KeySpaceList> branches) {
    if (branches == null || branches.isEmpty()) {
      return unsatisfiable(nDims);
    }
    List<KeySpace> combined = new ArrayList<>();
    for (KeySpaceList b : branches) {
      if (b.isEverything()) {
        return everything(nDims);
      }
      if (b.isUnsatisfiable()) {
        continue;
      }
      if (b.nDims != nDims) {
        throw new IllegalArgumentException(
          "KeySpaceList arity mismatch: " + nDims + " vs " + b.nDims);
      }
      combined.addAll(b.spaces);
    }
    return fromNormalized(nDims, combined);
  }

  /**
   * Folds pairwise merges in-place until no merge is possible.
   * <p>
   * <b>Algorithm.</b> Rule 2 of {@link KeySpace#unionIfMergeable} requires two spaces to
   * agree on N−1 dims. Equivalent spaces-up-to-one-dim are partitioned by a "signature"
   * that is the dim-tuple with one coordinate replaced by a wildcard. For arity N there
   * are N candidate wildcard positions; we try each in turn so any single disagreeing
   * dim can be found by a hash lookup rather than a quadratic scan.
   * <p>
   * Within a bucket (all spaces sharing N−1 coordinates) we sort the remaining dim's
   * ranges by lower bound and sweep left-to-right, merging overlapping/adjacent ranges.
   * <p>
   * Rule 1 (containment) is also handled by the bucket sweep: a range fully inside the
   * running merged range is absorbed. Containment across different signatures is rare
   * and not worth a quadratic check; the residual filter handles any over-approximation.
   * <p>
   * Complexity per pass: O(N · K · log K). Rounds converge in O(log K) because each
   * round halves (at worst) the number of non-mergeable groups. Total: O(N · K · (log K)²),
   * bounded and practical for K in the thousands.
   */
  private static void mergeToFixpoint(List<KeySpace> list) {
    if (list.size() < 2) {
      return;
    }
    int n = list.get(0).nDims();

    // Fast path for the overwhelmingly common single-PK-column OR case (e.g. `a = ? OR
    // a = ? OR ...` or `a IN (...)`): every KeySpace has exactly one constrained dim and
    // the rest are EVERYTHING. When every space in the list shares this shape on the
    // *same* constrained dim, the per-dim merge-fixpoint reduces to a single 1D
    // KeyRange.coalesce — identical to v1's orKeySlots. Skipping the hash-bucket /
    // Signature machinery here closes most of the v2/v1 gap on large OR chains.
    int onlyConstrainedDim = onlyConstrainedDim(list);
    if (onlyConstrainedDim >= 0) {
      mergeSingleDim(list, onlyConstrainedDim);
      return;
    }

    boolean progressed = true;
    int maxRounds = 64;
    while (progressed && maxRounds-- > 0) {
      progressed = false;
      // Only try wildcard positions where the list actually varies. If every space has the
      // same range on dim d, there's nothing to merge with wildcard=d (spaces already share
      // dim d, so the merge key would be identical to the d-included key — dedup handles
      // that). If every space shares dim d = EVERYTHING, wildcard=d wouldn't help either.
      // In practice for OR-of-equalities on a single PK column only one wildcard is
      // productive, and this check avoids N-1 wasted hash-bucket passes.
      boolean[] varies = dimsThatVary(list);
      for (int wildcard = 0; wildcard < n; wildcard++) {
        if (!varies[wildcard]) {
          continue;
        }
        if (mergeByWildcard(list, wildcard)) {
          progressed = true;
        }
      }
      // Also try the degenerate "no wildcard" case: two spaces fully equal collapse. This
      // catches cases where two branches produced the same KeySpace.
      if (dedupInPlace(list)) {
        progressed = true;
      }
    }
  }

  /**
   * If every space in the list has at most one non-EVERYTHING dim and they all agree on
   * which dim that is, returns that dim index; otherwise {@code -1}.
   */
  private static int onlyConstrainedDim(List<KeySpace> list) {
    int n = list.get(0).nDims();
    int sharedDim = -1;
    for (KeySpace ks : list) {
      int thisDim = -1;
      for (int d = 0; d < n; d++) {
        if (ks.get(d) != org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE) {
          if (thisDim != -1) {
            return -1; // more than one constrained dim
          }
          thisDim = d;
        }
      }
      if (thisDim == -1) {
        // A KeySpace that is fully EVERYTHING makes the whole OR EVERYTHING; caller
        // handles that upstream, but be defensive.
        return -1;
      }
      if (sharedDim == -1) {
        sharedDim = thisDim;
      } else if (sharedDim != thisDim) {
        return -1;
      }
    }
    return sharedDim;
  }

  /**
   * Specialized merge when every space constrains only dim {@code d}. Extracts the 1D
   * ranges, coalesces them, then writes the coalesced ranges back as single-dim
   * KeySpaces.
   * <p>
   * Strategy: lift each range to a single-dim KeySpace, then run the standard
   * mergeToFixpoint which uses {@link KeySpace#unionIfMergeable}'s correct disjoint-
   * singletons check. This avoids the {@link org.apache.phoenix.query.KeyRange#coalesce}
   * bug with inverted (DESC) singleton ranges where distinct points like `\xCD` ('2')
   * and `\xCD\xCC` ('23') get incorrectly merged because the underlying
   * {@code KeyRange.intersect} for inverted singletons computes a non-empty "backward"
   * range rather than EMPTY_RANGE.
   */
  private static void mergeSingleDim(List<KeySpace> list, int d) {
    int n = list.get(0).nDims();
    // Sort by the per-dim range lower-bound so adjacent ranges come together. Use
    // KeyRange's natural comparator.
    list.sort((a, b) -> org.apache.phoenix.query.KeyRange.COMPARATOR.compare(a.get(d), b.get(d)));
    // Sweep once, merging via unionIfMergeable.
    int write = 0;
    for (int read = 0; read < list.size(); read++) {
      KeySpace cur = list.get(read);
      if (write == 0) {
        list.set(write++, cur);
        continue;
      }
      KeySpace prev = list.get(write - 1);
      java.util.Optional<KeySpace> merged = prev.unionIfMergeable(cur);
      if (merged.isPresent()) {
        list.set(write - 1, merged.get());
      } else {
        list.set(write++, cur);
      }
    }
    while (list.size() > write) {
      list.remove(list.size() - 1);
    }
  }

  /**
   * Returns an N-length array where {@code varies[d]} is true iff the list contains at
   * least two distinct ranges on dim {@code d}. A single linear pass over the list.
   */
  private static boolean[] dimsThatVary(List<KeySpace> list) {
    int n = list.get(0).nDims();
    boolean[] varies = new boolean[n];
    KeySpace first = list.get(0);
    for (int i = 1; i < list.size(); i++) {
      KeySpace ks = list.get(i);
      for (int d = 0; d < n; d++) {
        if (!varies[d] && !ks.get(d).equals(first.get(d))) {
          varies[d] = true;
        }
      }
    }
    return varies;
  }

  /**
   * Groups spaces by their dim signature with dim {@code wildcard} excluded, then merges
   * the wildcard dim's ranges within each bucket via sort-and-sweep. Mutates {@code list}
   * in place. Returns {@code true} if any merge happened.
   */
  private static boolean mergeByWildcard(List<KeySpace> list, int wildcard) {
    if (list.size() < 2) {
      return false;
    }
    java.util.Map<org.apache.phoenix.compile.keyspace.KeySpace.Signature,
      java.util.List<KeySpace>> buckets = new java.util.HashMap<>();
    for (KeySpace ks : list) {
      org.apache.phoenix.compile.keyspace.KeySpace.Signature sig = ks.signatureExcluding(wildcard);
      buckets.computeIfAbsent(sig, s -> new ArrayList<>()).add(ks);
    }
    boolean merged = false;
    List<KeySpace> out = new ArrayList<>(list.size());
    for (java.util.List<KeySpace> bucket : buckets.values()) {
      if (bucket.size() == 1) {
        out.add(bucket.get(0));
        continue;
      }
      List<KeySpace> swept = sweepAndMerge(bucket, wildcard);
      if (swept.size() < bucket.size()) {
        merged = true;
      }
      out.addAll(swept);
    }
    if (merged) {
      list.clear();
      list.addAll(out);
    }
    return merged;
  }

  /**
   * Sort the wildcard-dim ranges and sweep left-to-right, merging overlapping/adjacent
   * pairs. All spaces in {@code bucket} share the other N−1 dims, so the result's
   * non-wildcard dims are just taken from any representative.
   */
  private static List<KeySpace> sweepAndMerge(List<KeySpace> bucket, int wildcard) {
    List<KeySpace> sorted = new ArrayList<>(bucket);
    sorted.sort((a, b) -> {
      org.apache.phoenix.query.KeyRange ra = a.get(wildcard);
      org.apache.phoenix.query.KeyRange rb = b.get(wildcard);
      if (ra.lowerUnbound()) {
        return rb.lowerUnbound() ? 0 : -1;
      }
      if (rb.lowerUnbound()) {
        return 1;
      }
      int cmp = org.apache.hadoop.hbase.util.Bytes.compareTo(ra.getLowerRange(), rb.getLowerRange());
      if (cmp != 0) {
        return cmp;
      }
      // For equal lowers, inclusive-lower comes first.
      return Boolean.compare(!ra.isLowerInclusive(), !rb.isLowerInclusive());
    });
    List<KeySpace> result = new ArrayList<>();
    KeySpace running = sorted.get(0);
    for (int i = 1; i < sorted.size(); i++) {
      KeySpace next = sorted.get(i);
      Optional<KeySpace> u = running.unionIfMergeable(next);
      if (u.isPresent()) {
        running = u.get();
      } else {
        result.add(running);
        running = next;
      }
    }
    result.add(running);
    return result;
  }

  /**
   * Removes exact duplicates while preserving order. Returns true if any duplicates were
   * removed. O(K) via a hash set.
   */
  private static boolean dedupInPlace(List<KeySpace> list) {
    if (list.size() < 2) {
      return false;
    }
    java.util.LinkedHashSet<KeySpace> set = new java.util.LinkedHashSet<>(list);
    if (set.size() == list.size()) {
      return false;
    }
    list.clear();
    list.addAll(set);
    return true;
  }

  private void requireSameArity(KeySpaceList other) {
    if (other.nDims != this.nDims) {
      throw new IllegalArgumentException(
        "KeySpaceList arity mismatch: " + this.nDims + " vs " + other.nDims);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeySpaceList)) {
      return false;
    }
    KeySpaceList that = (KeySpaceList) o;
    return this.nDims == that.nDims && this.spaces.equals(that.spaces);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] { nDims, spaces });
  }

  @Override
  public String toString() {
    if (isUnsatisfiable()) {
      return "KeySpaceList[UNSAT, n=" + nDims + "]";
    }
    return "KeySpaceList" + spaces;
  }
}
