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

package org.apache.phoenix.hbase.index.table;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * Reference to an HTableInterface. Currently, its pretty simple in that it is just a wrapper around
 * the table name.
 */
public class HTableInterfaceReference {

  private ImmutableBytesPtr tableName;


  public HTableInterfaceReference(ImmutableBytesPtr tableName) {
    this.tableName = tableName;
  }

  public ImmutableBytesPtr get() {
    return this.tableName;
  }

  public String getTableName() {
    return Bytes.toString(this.tableName.get(),this.tableName.getOffset(), this.tableName.getLength());
  }

  @Override
  public int hashCode() {
      return tableName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      HTableInterfaceReference other = (HTableInterfaceReference)obj;
      return tableName.equals(other.tableName);
  }

  @Override
  public String toString() {
    return Bytes.toString(this.tableName.get());
  }
}