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
package org.apache.phoenix.parse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.SortOrder;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

public class IndexKeyConstraint {
  public static final IndexKeyConstraint EMPTY =
    new IndexKeyConstraint(Collections.<Entry> emptyList());

  /** One column entry in an index-key constraint. */
  public static class Entry {
    private final ParseNode parseNode;
    private final SortOrder sortOrder;
    private final boolean dynamic;
    private final ColumnDef columnDef; // non-null iff dynamic

    private Entry(ParseNode parseNode, ColumnDef columnDef, SortOrder sortOrder,
        boolean dynamic) {
      this.parseNode = parseNode;
      this.columnDef = columnDef;
      this.sortOrder = sortOrder;
      this.dynamic = dynamic;
    }

    public static Entry regular(ParseNode parseNode, SortOrder sortOrder) {
      return new Entry(parseNode, null, sortOrder, false);
    }

    public static Entry dynamic(ParseNode parseNode, ColumnDef columnDef, SortOrder sortOrder) {
      if (columnDef == null) {
        throw new IllegalArgumentException("dynamic Entry requires a ColumnDef");
      }
      return new Entry(parseNode, columnDef, sortOrder, true);
    }

    public ParseNode getParseNode() { return parseNode; }
    public SortOrder getSortOrder() { return sortOrder; }
    public boolean isDynamic() { return dynamic; }
    public ColumnDef getColumnDef() { return columnDef; }
  }

  private final List<Entry> entries;

  IndexKeyConstraint(List<Entry> entries) {
    this.entries = ImmutableList.copyOf(entries);
  }

  public List<Entry> getEntries() {
    return entries;
  }

  /** Backwards-compatible accessor used by older callers. */
  public List<Pair<ParseNode, SortOrder>> getParseNodeAndSortOrderList() {
    List<Pair<ParseNode, SortOrder>> out = new ArrayList<>(entries.size());
    for (Entry e : entries) {
      out.add(Pair.newPair(e.getParseNode(), e.getSortOrder()));
    }
    return out;
  }

  public boolean isDynamic(int index) { return entries.get(index).isDynamic(); }
  public ColumnDef getColumnDef(int index) { return entries.get(index).getColumnDef(); }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (Entry entry : entries) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(entry.getParseNode().toString());
      if (entry.getSortOrder() != SortOrder.getDefault()) {
        sb.append(" " + entry.getSortOrder());
      }
    }
    return sb.toString();
  }
}
