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
package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Filter used when expressions reference to the entire row
 */
public class RowLevelFilter extends BooleanExpressionFilter {
  private boolean allVersions = false;
  private boolean keepRow = false;

  public RowLevelFilter() {
  }

  public RowLevelFilter(Expression expression, boolean allVersions) {
    super(expression);
    this.allVersions = allVersions;
  }

  @Override
  public void reset() {
    super.reset();
    keepRow = false;
  }

  // No @Override for HBase 3 compatibility
  public ReturnCode filterKeyValue(Cell v) {
    return filterCell(v);
  }

  @Override
  public ReturnCode filterCell(Cell v) {
    return allVersions ? ReturnCode.INCLUDE : ReturnCode.INCLUDE_AND_NEXT_COL;
  }

  @Override
  public void filterRowCells(List<Cell> kvs) throws IOException {
    Tuple tuple = new MultiKeyValueTuple();
    tuple.setKeyValues(kvs);
    keepRow = Boolean.TRUE.equals(evaluate(tuple));
  }

  @Override
  public boolean filterRow() {
    return !this.keepRow;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    allVersions = input.readBoolean();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeBoolean(allVersions);
  }

  public static RowLevelFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    try {
      return (RowLevelFilter) Writables.getWritable(pbBytes, new RowLevelFilter());
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }
}
