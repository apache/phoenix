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

package org.apache.phoenix.hbase.index.covered.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.hbase.index.covered.IndexCodec;

/**
 * Helper to build the configuration for the {@link CoveredColumnIndexer}.
 * <p>
 * This class is NOT thread-safe; all concurrent access must be managed externally.
 */
public class CoveredColumnIndexSpecifierBuilder {

  private static final String INDEX_TO_TABLE_CONF_PREFX = "hbase.index.covered.";
  // number of index 'groups'. Each group represents a set of 'joined' columns. The data stored with
  // each joined column are either just the columns in the group or all the most recent data in the
  // row (a fully covered index).
  private static final String COUNT = ".count";
  private static final String INDEX_GROUPS_COUNT_KEY = INDEX_TO_TABLE_CONF_PREFX + ".groups" + COUNT;
  private static final String INDEX_GROUP_PREFIX = INDEX_TO_TABLE_CONF_PREFX + "group.";
  private static final String INDEX_GROUP_COVERAGE_SUFFIX = ".columns";
  private static final String TABLE_SUFFIX = ".table";

  // right now, we don't support this should be easy enough to add later
  // private static final String INDEX_GROUP_FULLY_COVERED = ".covered";

  List<ColumnGroup> groups = new ArrayList<ColumnGroup>();
  private Map<String, String> specs = new HashMap<String, String>();

  /**
   * Add a group of columns to index
   * @param columns Pairs of cf:cq (full specification of a column) to index
   * @return the index of the group. This can be used to remove or modify the group via
   *         {@link #remove(int)} or {@link #get(int)}, any time before building
   */
  public int addIndexGroup(ColumnGroup columns) {
    if (columns == null || columns.size() == 0) {
      throw new IllegalArgumentException("Must specify some columns to index!");
    }
    int size = this.groups.size();
    this.groups.add(columns);
    return size;
  }

  public void remove(int i) {
    this.groups.remove(i);
  }

  public ColumnGroup get(int i) {
    return this.groups.get(i);
  }

  /**
   * Clear the stored {@link ColumnGroup}s for resuse.
   */
  public void reset() {
    this.groups.clear();
  }

  Map<String, String> convertToMap() {
    int total = this.groups.size();
    // hbase.index.covered.groups = i
    specs.put(INDEX_GROUPS_COUNT_KEY, Integer.toString(total));

    int i = 0;
    for (ColumnGroup group : groups) {
      addIndexGroupToSpecs(specs, group, i++);
    }

    return specs;
  }

  /**
   * @param specs
   * @param columns
   * @param index
   */
  private void addIndexGroupToSpecs(Map<String, String> specs, ColumnGroup columns, int index) {
    // hbase.index.covered.group.<i>
    String prefix = INDEX_GROUP_PREFIX + Integer.toString(index);

    // set the table to which the group writes
    // hbase.index.covered.group.<i>.table
    specs.put(prefix + TABLE_SUFFIX, columns.getTable());
    
    // a different key for each column in the group
    // hbase.index.covered.group.<i>.columns
    String columnPrefix = prefix + INDEX_GROUP_COVERAGE_SUFFIX;
    // hbase.index.covered.group.<i>.columns.count = <j>
    String columnsSizeKey = columnPrefix + COUNT;
    specs.put(columnsSizeKey, Integer.toString(columns.size()));
    
    // add each column in the group
    int i=0; 
    for (CoveredColumn column : columns) {
      // hbase.index.covered.group.<i>.columns.<j>
      String nextKey = columnPrefix + "." + Integer.toString(i);
      String nextValue = column.serialize();
      specs.put(nextKey, nextValue);
      i++;
    }
  }

  public void build(HTableDescriptor desc) throws IOException {
    build(desc, CoveredColumnIndexCodec.class);
  }

  void build(HTableDescriptor desc, Class<? extends IndexCodec> clazz) throws IOException {
    // add the codec for the index to the map of options
    Map<String, String> opts = this.convertToMap();
    opts.put(CoveredColumnsIndexBuilder.CODEC_CLASS_NAME_KEY, clazz.getName());
    Indexer.enableIndexing(desc, CoveredColumnIndexer.class, opts, Coprocessor.PRIORITY_USER);
  }

  static List<ColumnGroup> getColumns(Configuration conf) {
    int count= conf.getInt(INDEX_GROUPS_COUNT_KEY, 0);
    if (count ==0) {
      return Collections.emptyList();
    }

    // parse out all the column groups we should index
    List<ColumnGroup> columns = new ArrayList<ColumnGroup>(count);
    for (int i = 0; i < count; i++) {
      // parse out each group
      String prefix = INDEX_GROUP_PREFIX + i;

      // hbase.index.covered.group.<i>.table
      String table = conf.get(prefix + TABLE_SUFFIX);
      ColumnGroup group = new ColumnGroup(table);

      // parse out each column in the group
      // hbase.index.covered.group.<i>.columns
      String columnPrefix = prefix + INDEX_GROUP_COVERAGE_SUFFIX;
      // hbase.index.covered.group.<i>.columns.count = j
      String columnsSizeKey = columnPrefix + COUNT;
      int columnCount = conf.getInt(columnsSizeKey, 0);
      for(int j=0; j< columnCount; j++){
        String columnKey = columnPrefix + "." + j;
        CoveredColumn column = CoveredColumn.parse(conf.get(columnKey));
        group.add(column);
      }

      // add the group
      columns.add(group);
    }
    return columns;
  }

  /**
   * @param key
   * @param value
   */
  public void addArbitraryConfigForTesting(String key, String value) {
    this.specs.put(key, value);
  }
}