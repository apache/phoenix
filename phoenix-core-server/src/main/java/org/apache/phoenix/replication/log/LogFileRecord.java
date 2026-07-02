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
package org.apache.phoenix.replication.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.replication.MutationCellGrouper;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class LogFileRecord implements LogFile.Record {

  private String tableName;
  private long commitId;
  private List<Cell> cells = Collections.emptyList();
  private Map<String, byte[]> attributes = Collections.emptyMap();
  private int serializedLength;

  public LogFileRecord() {
  }

  @Override
  public String getHBaseTableName() {
    return tableName;
  }

  @Override
  public LogFile.Record setHBaseTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  @Override
  public long getCommitId() {
    return commitId;
  }

  @Override
  public LogFile.Record setCommitId(long commitId) {
    this.commitId = commitId;
    return this;
  }

  @Override
  public List<Cell> getCells() {
    return cells;
  }

  @Override
  public LogFile.Record setCells(List<Cell> cells) {
    Preconditions.checkNotNull(cells, "cells must not be null");
    this.cells = cells;
    return this;
  }

  @Override
  public Map<String, byte[]> getAttributes() {
    return attributes;
  }

  @Override
  public LogFile.Record setAttributes(Map<String, byte[]> attributes) {
    Preconditions.checkNotNull(attributes, "attributes must not be null");
    this.attributes = attributes;
    return this;
  }

  @Override
  public List<Mutation> getMutations() throws IOException {
    List<Mutation> result = MutationCellGrouper.splitCellsIntoMutations(cells);
    if (!attributes.isEmpty()) {
      for (Mutation m : result) {
        for (Map.Entry<String, byte[]> e : attributes.entrySet()) {
          m.setAttribute(e.getKey(), e.getValue());
        }
      }
    }
    return result;
  }

  @Override
  public Mutation getMutation() throws IOException {
    List<Mutation> mutations = getMutations();
    if (mutations.size() != 1) {
      throw new IllegalStateException("Record does not contain exactly one mutation (count="
        + mutations.size() + "); use getMutations() instead");
    }
    return mutations.get(0);
  }

  @Override
  public LogFile.Record setMutation(Mutation mutation) {
    List<Cell> body = new ArrayList<>();
    for (List<Cell> familyCells : mutation.getFamilyCellMap().values()) {
      body.addAll(familyCells);
    }
    this.cells = body;
    this.attributes = Collections.emptyMap();
    return this;
  }

  @Override
  public int getSerializedLength() {
    // NOTE: Should be set by the Codec using setSerializedLength after reading or writing
    // the record.
    return this.serializedLength;
  }

  @Override
  public LogFile.Record setSerializedLength(int serializedLength) {
    this.serializedLength = serializedLength;
    return this;
  }

  @Override
  public String toString() {
    return "LogFileRecord [tableName=" + tableName + ", commitId=" + commitId + ", cellCount="
      + cells.size() + ", attrCount=" + attributes.size() + "]";
  }

}
