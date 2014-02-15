/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.covered;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.apache.phoenix.hbase.index.covered.CoveredColumns;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;

public class TestCoveredColumns {

  private static final byte[] fam = Bytes.toBytes("fam");
  private static final byte[] qual = Bytes.toBytes("qual");

  @Test
  public void testCovering() {
    ColumnReference ref = new ColumnReference(fam, qual);
    CoveredColumns columns = new CoveredColumns();
    assertEquals("Should have only found a single column to cover", 1, columns
        .findNonCoveredColumns(Arrays.asList(ref)).size());

    columns.addColumn(ref);
    assertEquals("Shouldn't have any columns to cover", 0,
      columns.findNonCoveredColumns(Arrays.asList(ref)).size());
  }
}