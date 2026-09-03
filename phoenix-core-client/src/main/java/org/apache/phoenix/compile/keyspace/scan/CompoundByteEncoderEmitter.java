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
package org.apache.phoenix.compile.keyspace.scan;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.keyspace.KeySpace;
import org.apache.phoenix.compile.keyspace.KeySpaceList;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.RowKeySchema;

/**
 * Overrides a {@link Scan}'s start/stop rows with bytes produced by
 * {@link CompoundByteEncoder}, using {@link CompoundByteEncoder#encodeListLower} /
 * {@link CompoundByteEncoder#encodeListUpper} for the user-tail and prepending the caller's
 * prefix bytes (salt/viewIndexId/tenantId).
 * <p>
 * Applies only to shapes within the encoder's proven envelope, as established by
 * {@link CompoundByteEncoderDifferentialTest} (ASC fields, no IS_NULL/IS_NOT_NULL
 * sentinels) and {@link CompoundByteEncoderListDifferentialTest} (multi-space
 * byte-lex-min/max bounding envelope). For out-of-envelope shapes the caller leaves the
 * classical scan bytes in place.
 * <p>
 * The {@link org.apache.phoenix.compile.ScanRanges} emitted by the existing path still
 * drives the {@link org.apache.phoenix.filter.SkipScanFilter} and point-lookup
 * classification — only the row bytes on the {@link Scan} are replaced. This narrows the
 * scan to exactly the envelope the encoder defines while preserving downstream machinery
 * unchanged.
 */
public final class CompoundByteEncoderEmitter {

  private CompoundByteEncoderEmitter() {
  }

  /**
   * Returns {@code true} iff the encoder should override the scan's row bytes for this
   * {@link KeySpaceList}.
   * <p>
   * The encoder emits bytes per its own well-defined rules (separator rules, nextKey
   * bumps, per-dim encoding with DESC terminator on exclusive-lower var-width). It does
   * not mimic V1's {@code ScanUtil.setKey} tail-strip — that's a historical V1 artifact
   * tied to V1's compound-slot packing, not a semantic correctness requirement. For any
   * row, the encoder's output and V1's output admit the same rows; they may differ on
   * whether a trailing separator byte is included.
   * <p>
   * Covers single-space and multi-space lists, ASC and DESC. Single-space: encoder's
   * bytes are semantically equivalent to V1's — tests that assert specific byte shapes
   * have been updated to the encoder form. Multi-space: the encoder's list envelope
   * (byte-lex-min/max across per-space encodings) preserves cross-dim tuple correlation
   * that V1's per-slot projection loses — this is the fix for
   * {@code testRVCScanBoundaries1/2}.
   * <p>
   * Exclusions:
   * <ul>
   * <li>IS_NULL / IS_NOT_NULL sentinels — ScanUtil has dedicated paths the encoder doesn't
   *     reproduce.</li>
   * <li>Salted tables — salt bytes are computed per row-key (hash mod nBuckets), not
   *     known statically. V1's ScanRanges.create path recognizes point-lookups on salted
   *     tables and computes per-key salt bytes via SaltingUtil. The encoder's
   *     overrideScanRows uses a single static salt prefix (0x00), which would only match
   *     rows in bucket 0 — missing rows hashed into buckets 1-3. Defer to the classical
   *     path for salted tables until encoder gains salt-aware point-lookup handling.</li>
   * </ul>
   */
  public static boolean isInScope(KeySpaceList list, RowKeySchema schema, int prefixSlots,
    boolean isSalted) {
    if (list == null || list.isUnsatisfiable() || list.isEverything()) {
      return false;
    }
    if (isSalted) {
      return false;
    }
    // Skip when every space has EVERYTHING at the leading user slot. The classical
    // {@link ScanRanges} path produces the correct unbounded startRow for this shape;
    // overriding it here would emit a spurious leading separator byte ({@code \x00})
    // for variable-width leading columns, which HBase-side offset/paging logic treats
    // as a non-empty startRow. That alters region-boundary scan resume semantics and
    // produces wrong results under server OFFSET + region moves
    // (see ServerPagingWithRegionMovesIT.testLimitOffset*).
    if (prefixSlots < schema.getFieldCount()) {
      boolean allLeadingEverything = true;
      for (KeySpace s : list.spaces()) {
        if (prefixSlots >= s.nDims()) {
          allLeadingEverything = false;
          break;
        }
        if (s.get(prefixSlots) != KeyRange.EVERYTHING_RANGE) {
          allLeadingEverything = false;
          break;
        }
      }
      if (allLeadingEverything) {
        return false;
      }
    }
    for (KeySpace s : list.spaces()) {
      for (int d = prefixSlots; d < s.nDims(); d++) {
        KeyRange r = s.get(d);
        if (r == KeyRange.IS_NULL_RANGE || r == KeyRange.IS_NOT_NULL_RANGE) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Override {@code scan.startRow} / {@code scan.stopRow} with the encoder's bytes
   * prepended by {@code prefixBytes}. When the encoder returns {@link KeyRange#UNBOUND}
   * for a bound, the scan's existing row for that bound is kept — it already reflects
   * whatever the classical path computed (typically {@code UNBOUND} itself for that side).
   */
  public static void overrideScanRows(Scan scan, KeySpaceList list, RowKeySchema schema,
    int prefixSlots, byte[] prefixBytes) {
    byte[] lower = CompoundByteEncoder.encodeListLower(schema, list, prefixSlots);
    byte[] upper = CompoundByteEncoder.encodeListUpper(schema, list, prefixSlots);
    if (lower != KeyRange.UNBOUND && lower.length > 0) {
      scan.withStartRow(concat(prefixBytes, lower));
    }
    if (upper != KeyRange.UNBOUND && upper.length > 0) {
      scan.withStopRow(concat(prefixBytes, upper));
    }
  }

  private static byte[] concat(byte[] prefix, byte[] tail) {
    if (prefix == null || prefix.length == 0) {
      return tail;
    }
    byte[] out = new byte[prefix.length + tail.length];
    System.arraycopy(prefix, 0, out, 0, prefix.length);
    System.arraycopy(tail, 0, out, prefix.length, tail.length);
    return out;
  }
}
