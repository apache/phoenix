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
package org.apache.phoenix.hbase.index.covered.update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Simple POJO for tracking a bunch of column references and the next-newest timestamp for those
 * columns
 * <p>
 * Two {@link ColumnTracker}s are considered equal if they track the same columns, even if their
 * timestamps are different.
 */
public class ColumnTracker implements IndexedColumnGroup {

  public static final long NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP = Long.MAX_VALUE;
  public static final long GUARANTEED_NEWER_UPDATES = Long.MIN_VALUE;
  private final List<ColumnReference> columns;
  private long ts = NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
  private final int hashCode;

  private static int calcHashCode(List<ColumnReference> columns) {
      return columns.hashCode();
    }

  public ColumnTracker(Collection<? extends ColumnReference> columns) {
    this.columns = new ArrayList<ColumnReference>(columns);
    // sort the columns
    Collections.sort(this.columns);
    this.hashCode = calcHashCode(this.columns);
  }

  /**
   * Set the current timestamp, only if the passed timestamp is strictly less than the currently
   * stored timestamp
   * @param ts the timestmap to potentially store.
   * @return the currently stored timestamp.
   */
  public long setTs(long ts) {
    this.ts = this.ts > ts ? ts : this.ts;
    return this.ts;
  }

  public long getTS() {
    return this.ts;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o){
    if(!(o instanceof ColumnTracker)){
      return false;
    }
    ColumnTracker other = (ColumnTracker)o;
    if (hashCode != other.hashCode) {
        return false;
    }
    if (other.columns.size() != columns.size()) {
      return false;
    }

    // check each column to see if they match
    for (int i = 0; i < columns.size(); i++) {
      if (!columns.get(i).equals(other.columns.get(i))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public List<ColumnReference> getColumns() {
    return this.columns;
  }

  /**
   * @return <tt>true</tt> if this set of columns has seen a column with a timestamp newer than the
   *         requested timestamp, <tt>false</tt> otherwise.
   */
  public boolean hasNewerTimestamps() {
    return !isNewestTime(this.ts);
  }

  /**
   * @param ts timestamp to check
   * @return <tt>true</tt> if the timestamp is at the most recent timestamp for a column
   */
  public static boolean isNewestTime(long ts) {
    return ts == NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP;
  }
}