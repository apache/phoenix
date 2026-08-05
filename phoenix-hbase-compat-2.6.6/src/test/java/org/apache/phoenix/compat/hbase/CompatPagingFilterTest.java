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
package org.apache.phoenix.compat.hbase;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class CompatPagingFilterTest {

  /** Minimal concrete subclass so the abstract compat base can be instantiated. */
  private static class TestPagingFilter extends CompatPagingFilter {
    TestPagingFilter(Filter delegate) {
      super(delegate);
    }
  }

  private static class HintingDelegate extends FilterBase {
    final Cell rejectedRowHint = new KeyValue(Bytes.toBytes("rejected"), Bytes.toBytes("f"),
      Bytes.toBytes("q"), Bytes.toBytes("v"));
    final Cell skipHint = new KeyValue(Bytes.toBytes("skip"), Bytes.toBytes("f"),
      Bytes.toBytes("q"), Bytes.toBytes("v"));

    @Override
    public Cell getHintForRejectedRow(Cell firstRowCell) throws IOException {
      return rejectedRowHint;
    }

    @Override
    public Cell getSkipHint(Cell skippedCell) throws IOException {
      return skipHint;
    }
  }

  @Test
  public void forwardsToDelegateWhenPresent() throws IOException {
    HintingDelegate delegate = new HintingDelegate();
    TestPagingFilter filter = new TestPagingFilter(delegate);
    assertSame(delegate.rejectedRowHint, filter.getHintForRejectedRow(null));
    assertSame(delegate.skipHint, filter.getSkipHint(null));
  }

  @Test
  public void returnsNullWhenDelegateAbsent() throws IOException {
    TestPagingFilter filter = new TestPagingFilter(null);
    assertNull(filter.getHintForRejectedRow(null));
    assertNull(filter.getSkipHint(null));
  }
}
