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
package org.apache.phoenix.expression.function;

import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.ScanStartKeyParseNode;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;

@BuiltInFunction(name = ScanStartKeyFunction.NAME, args = {},
    nodeClass = ScanStartKeyParseNode.class)
public class ScanStartKeyFunction extends ScalarFunction {

  public static final String NAME = "SCAN_START_KEY";

  public ScanStartKeyFunction(List<Expression> children) {
    super(children);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType getDataType() {
    return PVarbinary.INSTANCE;
  }

  @Override
  public boolean isStateless() {
    return false;
  }

  @Override
  public Determinism getDeterminism() {
    return Determinism.PER_ROW;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    if (tuple != null) {
      Cell cell = tuple.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("0"));
      ptr.set(CellUtil.cloneValue(cell));
      return true;
    }
    return false;
  }
}
