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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * A collection of {@link CoveredColumn}s that should be included in a covered index.
 */
public class ColumnGroup implements Iterable<CoveredColumn> {

  private List<CoveredColumn> columns = new ArrayList<CoveredColumn>();
  private String table;

  public ColumnGroup(String tableName) {
    this.table = tableName;
  }

  public void add(CoveredColumn column) {
    this.columns.add(column);
  }

  public String getTable() {
    return table;
  }

  /**
   * Check to see if any {@link CoveredColumn} in <tt>this</tt> matches the given family
   * @param family to check
   * @return <tt>true</tt> if any column covers the family
   */
  public boolean matches(String family) {
    for (CoveredColumn column : columns) {
      if (column.matchesFamily(family)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Check to see if any column matches the family/qualifier pair
   * @param family family to match against
   * @param qualifier qualifier to match, can be <tt>null</tt>, in which case we match all
   *          qualifiers
   * @return <tt>true</tt> if any column matches, <tt>false</tt> otherwise
   */
  public boolean matches(byte[] family, byte[] qualifier) {
    // families are always printable characters
    String fam = Bytes.toString(family);
    for (CoveredColumn column : columns) {
      if (column.matchesFamily(fam)) {
        // check the qualifier
          if (column.matchesQualifier(qualifier)) {
            return true;
        }
      }
    }
    return false;
  }

  /**
   * @return the number of columns in the group
   */
  public int size() {
    return this.columns.size();
  }

  @Override
  public Iterator<CoveredColumn> iterator() {
    return columns.iterator();
  }

  /**
   * @param index index of the column to get
   * @return the specified column
   */
  public CoveredColumn getColumnForTesting(int index) {
    return this.columns.get(index);
  }

  @Override
  public String toString() {
    return "ColumnGroup - table: " + table + ", columns: " + columns;
  }

  public List<CoveredColumn> getColumns() {
    return this.columns;
  }
}