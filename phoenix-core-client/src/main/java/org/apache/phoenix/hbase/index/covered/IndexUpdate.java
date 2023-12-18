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

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;

/**
 * Update to make to the index table.
 */
public class IndexUpdate {
  Mutation update;
  byte[] tableName;
  ColumnTracker columns;

  public IndexUpdate(ColumnTracker tracker) {
    this.columns = tracker;
  }

  public void setUpdate(Mutation p) {
    this.update = p;
  }

  public void setTable(byte[] tableName) {
    this.tableName = tableName;
  }

  public Mutation getUpdate() {
    return update;
  }

  public byte[] getTableName() {
    return tableName;
  }

  public ColumnTracker getIndexedColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "IndexUpdate: \n\ttable - " + Bytes.toString(tableName) + "\n\tupdate: " + update
        + "\n\tcolumns: " + columns;
  }

  public static IndexUpdate createIndexUpdateForTesting(ColumnTracker tracker, byte[] table, Put p) {
    IndexUpdate update = new IndexUpdate(tracker);
    update.setTable(table);
    update.setUpdate(p);
    return update;
  }

  /**
   * @return <tt>true</tt> if the necessary state for a valid index update has been set.
   */
  public boolean isValid() {
    return this.tableName != null && this.update != null;
  }
}