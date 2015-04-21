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

package org.apache.phoenix.hbase.index.scanner;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import org.apache.phoenix.hbase.index.covered.KeyValueStore;
import org.apache.phoenix.hbase.index.covered.filter.ApplyAndFilterDeletesFilter;
import org.apache.phoenix.hbase.index.covered.filter.ColumnTrackingNextLargestTimestampFilter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 *
 */
public class ScannerBuilder {

  private KeyValueStore memstore;
  private Mutation update;


  public ScannerBuilder(KeyValueStore memstore, Mutation update) {
    this.memstore = memstore;
    this.update = update;
  }

  public Scanner buildIndexedColumnScanner(Collection<? extends ColumnReference> indexedColumns, ColumnTracker tracker, long ts) {

    Filter columnFilters = getColumnFilters(indexedColumns);
    FilterList filters = new FilterList(Lists.newArrayList(columnFilters));

    // skip to the right TS. This needs to come before the deletes since the deletes will hide any
    // state that comes before the actual kvs, so we need to capture those TS as they change the row
    // state.
    filters.addFilter(new ColumnTrackingNextLargestTimestampFilter(ts, tracker));

    // filter out kvs based on deletes
    filters.addFilter(new ApplyAndFilterDeletesFilter(getAllFamilies(indexedColumns)));

    // combine the family filters and the rest of the filters as a
    return getFilteredScanner(filters);
  }

  /**
   * @param columns columns to filter
   * @return filter that will skip any {@link KeyValue} that doesn't match one of the passed columns
   *         and the
   */
  private Filter
      getColumnFilters(Collection<? extends ColumnReference> columns) {
    // each column needs to be added as an OR, so we need to separate them out
    FilterList columnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);

    // create a filter that matches each column reference
    for (ColumnReference ref : columns) {
      Filter columnFilter =
          new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(ref.getFamily()));
      // combine with a match for the qualifier, if the qualifier is a specific qualifier
      if (!Bytes.equals(ColumnReference.ALL_QUALIFIERS, ref.getQualifier())) {
        columnFilter =
            new FilterList(columnFilter, new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(
                ref.getQualifier())));
      }
      columnFilters.addFilter(columnFilter);
    }
    return columnFilters;
  }

  private Set<ImmutableBytesPtr>
      getAllFamilies(Collection<? extends ColumnReference> columns) {
    Set<ImmutableBytesPtr> families = new HashSet<ImmutableBytesPtr>();
    for (ColumnReference ref : columns) {
      families.add(ref.getFamilyWritable());
    }
    return families;
  }

  private Scanner getFilteredScanner(Filter filters) {
    // create a scanner and wrap it as an iterator, meaning you can only go forward
    final FilteredKeyValueScanner kvScanner = new FilteredKeyValueScanner(filters, memstore);
    // seek the scanner to initialize it
    KeyValue start = KeyValueUtil.createFirstOnRow(update.getRow());
    try {
      if (!kvScanner.seek(start)) {
        return new EmptyScanner();
      }
    } catch (IOException e) {
      // This should never happen - everything should explode if so.
      throw new RuntimeException(
          "Failed to seek to first key from update on the memstore scanner!", e);
    }

    // we have some info in the scanner, so wrap it in an iterator and return.
    return new Scanner() {

      @Override
      public Cell next() {
        try {
          return kvScanner.next();
        } catch (IOException e) {
          throw new RuntimeException("Error reading kvs from local memstore!");
        }
      }

      @Override
      public boolean seek(KeyValue next) throws IOException {
        // check to see if the next kv is after the current key, in which case we can use reseek,
        // which will be more efficient
        Cell peek = kvScanner.peek();
        // there is another value and its before the requested one - we can do a reseek!
        if (peek != null) {
          int compare = KeyValue.COMPARATOR.compare(peek, next);
          if (compare < 0) {
            return kvScanner.reseek(next);
          } else if (compare == 0) {
            // we are already at the given key!
            return true;
          }
        }
        return kvScanner.seek(next);
      }

      @Override
      public Cell peek() throws IOException {
        return kvScanner.peek();
      }

      @Override
      public void close() {
        kvScanner.close();
      }
    };
  }
}