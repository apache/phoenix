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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

/**
 * A collection of {@link KeyValue KeyValues} to the primary table
 */
public class Batch {

  private static final long pointDeleteCode = KeyValue.Type.Delete.getCode();
  private final long timestamp;
  private List<Cell> batch = new ArrayList<Cell>();
  private boolean allPointDeletes = true;

  /**
   * @param ts
   */
  public Batch(long ts) {
    this.timestamp = ts;
  }

  public void add(Cell kv){
    if (pointDeleteCode != kv.getTypeByte()) {
      allPointDeletes = false;
    }
    batch.add(kv);
  }

  public boolean isAllPointDeletes() {
    return allPointDeletes;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public List<Cell> getKvs() {
    return this.batch;
  }
}