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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.RowSizeParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PUnsignedLong;

/**
 * Function to return the total size of the HBase cells that constitute a given row
 */
@BuiltInFunction(name = RowSizeFunction.NAME, nodeClass = RowSizeParseNode.class, args = {})
public class RowSizeFunction extends ScalarFunction {

  public static final String NAME = "ROW_SIZE";

  public RowSizeFunction() {
  }

  public RowSizeFunction(List<Expression> children) {
    super(children);
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    if (tuple == null) {
      return false;
    }
    long size = 0;
    for (int i = 0; i < tuple.size(); i++) {
      size += tuple.getValue(i).getSerializedSize();
    }
    ptr.set(PUnsignedLong.INSTANCE.toBytes(size));
    return true;
  }

  @Override
  public PDataType getDataType() {
    return PUnsignedLong.INSTANCE;
  }

  @Override
  public String getName() {
    return NAME;
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
  public boolean isRowLevel() {
    return true;
  }
}
