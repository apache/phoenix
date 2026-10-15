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
package org.apache.phoenix.filter;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compat.hbase.CompatDelegateFilter;
import org.junit.Test;

/**
 * Locks in that Phoenix's delegating filters extend the compat base class, so that the
 * version-gated getSkipHint / getHintForRejectedRow forwarding added in the 2.6.6 compat module is
 * inherited. Intentionally structural only (no calls to the new Filter methods) so it compiles
 * under every hbase.profile, including profiles whose HBase lacks those methods.
 */
public class DelegateFilterStructureTest {

  @Test
  public void delegateFilterExtendsCompatDelegateFilter() {
    DelegateFilter f = new DelegateFilter(null);
    assertTrue(f instanceof CompatDelegateFilter);
  }

  @Test
  public void allVersionsIndexRebuildFilterInheritsCompatDelegateFilter() {
    // AllVersionsIndexRebuildFilter(Filter originalFilter) — single-arg ctor.
    AllVersionsIndexRebuildFilter f = new AllVersionsIndexRebuildFilter(null);
    assertTrue(f instanceof CompatDelegateFilter);
  }

  @Test
  public void unverifiedRowFilterInheritsCompatDelegateFilter() {
    // UnverifiedRowFilter(Filter delegate, byte[] emptyCF, byte[] emptyCQ) — the ctor
    // Preconditions-checks the two byte[] args are non-null, so pass real (empty) arrays.
    // A null delegate is fine: it is only stored, not dereferenced, by the constructor.
    UnverifiedRowFilter f = new UnverifiedRowFilter(null, Bytes.toBytes("cf"), Bytes.toBytes("cq"));
    assertTrue(f instanceof CompatDelegateFilter);
  }
}
