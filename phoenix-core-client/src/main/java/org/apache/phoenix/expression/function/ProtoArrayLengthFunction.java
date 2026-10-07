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

import java.io.DataInput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.proto.PathPlan;
import org.apache.phoenix.expression.util.proto.ProtoPathResolver;
import org.apache.phoenix.expression.util.proto.ProtoPathWalker;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ProtoArrayLengthParseNode;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * {@code PROTO_ARRAY_LENGTH(payload, path)} — returns the count of elements in a repeated field or
 * entries in a map at the given path. Returns 0 when the ancestor container is not present
 * (child-of-unset-parent) or the target field has zero occurrences.
 */
@FunctionParseNode.BuiltInFunction(name = ProtoArrayLengthFunction.NAME,
    nodeClass = ProtoArrayLengthParseNode.class,
    args = { @FunctionParseNode.Argument(allowedTypes = { PProtobuf.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true), })
public class ProtoArrayLengthFunction extends ScalarFunction {

  public static final String NAME = "PROTO_ARRAY_LENGTH";

  private volatile PathPlan cachedPlan;
  private final ProtoDescriptorRegistry registry;

  public ProtoArrayLengthFunction() {
    this.registry = null;
  }

  public ProtoArrayLengthFunction(List<Expression> children) {
    this(children, null);
  }

  /** @param registry the compiling connection's; {@code null} falls back to the per-JVM one */
  public ProtoArrayLengthFunction(List<Expression> children, ProtoDescriptorRegistry registry) {
    super(children);
    this.registry = registry;
  }

  @Override
  public ProtoArrayLengthFunction clone(List<Expression> children) {
    return new ProtoArrayLengthFunction(children, registry);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType<?> getDataType() {
    return PInteger.INSTANCE;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    this.cachedPlan = null;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    if (!ProtoFunctionsUtil.evaluatePayload(getChildren().get(0), tuple, ptr)) {
      return false;
    }
    byte[] buf = ptr.get();
    int offset = ptr.getOffset();
    int length = ptr.getLength();

    PathPlan plan;
    try {
      plan = ensurePlan();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    int count;
    try {
      count = ProtoPathWalker.walkForCount(buf, offset, length, plan);
    } catch (IOException e) {
      ptr.set(PInteger.INSTANCE.toBytes(0));
      return true;
    }
    ptr.set(PInteger.INSTANCE.toBytes(count));
    return true;
  }

  public PathPlan ensurePlan() throws SQLException {
    PathPlan snapshot = cachedPlan;
    if (snapshot != null) {
      return snapshot;
    }
    synchronized (this) {
      snapshot = cachedPlan;
      if (snapshot != null) {
        return snapshot;
      }
      PathPlan plan = ProtoFunctionsUtil.compilePathPlan(getChildren(), NAME, registry,
        ProtoDescriptorRegistry.PlanKind.ARRAY_LENGTH, null,
        ProtoPathResolver::resolveForArrayLength);
      cachedPlan = plan;
      return plan;
    }
  }
}
