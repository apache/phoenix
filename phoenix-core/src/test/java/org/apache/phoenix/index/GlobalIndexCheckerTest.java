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
package org.apache.phoenix.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.phoenix.filter.DistinctPrefixFilter;
import org.junit.Test;

public class GlobalIndexCheckerTest {

  @Test
  public void testShareDistinctPrefixFilterKeepsReverseDirectionOfNestedList() {
    // The main scan is reversed, so the shared filter is reversed.
    DistinctPrefixFilter sharedFilter = new DistinctPrefixFilter();
    sharedFilter.setReversed(true);
    // The copy of the client filter has a nested list and the direction of the main scan.
    FilterList copy =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, new KeyOnlyFilter(), new FilterList(
        FilterList.Operator.MUST_PASS_ALL, new FirstKeyOnlyFilter(), new DistinctPrefixFilter()));
    copy.setReversed(true);

    Filter result = GlobalIndexChecker.shareDistinctPrefixFilter(copy, sharedFilter);

    assertTrue(result instanceof FilterList);
    FilterList outerList = (FilterList) result;
    assertTrue(outerList.isReversed());
    List<Filter> outerFilters = outerList.getFilters();
    assertEquals(2, outerFilters.size());
    assertTrue(outerFilters.get(1) instanceof FilterList);
    FilterList nestedList = (FilterList) outerFilters.get(1);
    assertTrue(nestedList.isReversed());
    assertSame(sharedFilter, nestedList.getFilters().get(1));
  }
}
