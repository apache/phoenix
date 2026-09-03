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

import org.apache.phoenix.compile.keyspace.KeySpaceList;

/**
 * V2-owned metadata carried from {@link V2ScanBuilder} through the
 * {@code StatementContext} to downstream components that benefit from the pre-encoding,
 * mathematical shape of the scan (currently: the explain-plan formatter).
 * <p>
 * The plan display reads {@code ScanRanges.getRanges()} by default and decodes each
 * slot's bytes through the schema. When V2's compound emission pre-bumps an
 * inclusive-upper range via {@code nextKey(...)} so that {@code col <= 1} becomes a
 * byte-exclusive {@code [_, 0x82)}, the display decodes the upper as {@code 2} rather
 * than {@code 1}. The scan bytes are identical on the wire; only the display differs.
 * This artifact lets the formatter read the {@link KeySpaceList} directly — the logical
 * model that has not been byte-bumped — and render {@code [_, 1]} verbatim. The
 * {@link ScanRanges} the context also holds continues to drive actual scan execution.
 * <p>
 * {@code WhereOptimizerV2.run} attaches one instance per scan (when the optimizer
 * produced a non-EVERYTHING narrowing). Consumers that know about V2 prefer it;
 * consumers that don't read the underlying {@link ScanRanges} and are unaffected.
 */
public final class V2ScanArtifact {

  private final KeySpaceList list;
  private final int nPkColumns;
  private final int prefixSlots;

  public V2ScanArtifact(KeySpaceList list, int nPkColumns, int prefixSlots) {
    this.list = list;
    this.nPkColumns = nPkColumns;
    this.prefixSlots = prefixSlots;
  }

  /** Post-normalization, post-AND/OR-fixpoint key-space list. */
  public KeySpaceList list() {
    return list;
  }

  /** Total number of PK columns on the table (including prefix columns). */
  public int nPkColumns() {
    return nPkColumns;
  }

  /**
   * Number of prefix PK columns not modeled in the {@link KeySpaceList}: salt byte +
   * viewIndexId + tenantId. The {@link KeySpaceList}'s dim 0 corresponds to PK column
   * {@code prefixSlots} in the full schema.
   */
  public int prefixSlots() {
    return prefixSlots;
  }
}
