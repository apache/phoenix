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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ProtoFieldTypeParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * {@code PROTO_FIELD_TYPE(payload, path)} — returns the Phoenix SQL type name for the leaf at the
 * given path.
 */
@FunctionParseNode.BuiltInFunction(name = ProtoFieldTypeFunction.NAME,
    nodeClass = ProtoFieldTypeParseNode.class,
    args = { @FunctionParseNode.Argument(allowedTypes = { PProtobuf.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true), })
public class ProtoFieldTypeFunction extends ScalarFunction {

  public static final String NAME = "PROTO_FIELD_TYPE";

  public ProtoFieldTypeFunction() {
  }

  public ProtoFieldTypeFunction(List<Expression> children) {
    super(children);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType<?> getDataType() {
    return PVarchar.INSTANCE;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    return getChildren().get(getChildren().size() - 1).evaluate(tuple, ptr);
  }
}
