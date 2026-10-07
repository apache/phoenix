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
import org.apache.phoenix.parse.ProtoExistsParseNode;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

/**
 * {@code PROTO_EXISTS(payload, path)} — returns TRUE if the leaf at the given path is set on the
 * wire; FALSE for absent leaves (proto3 implicit defaults, unset optional / proto2, missing map
 * keys, past-end indices, child-of-unset-parent, malformed payloads); SQL NULL only when the
 * payload column value itself is SQL NULL.
 */
@FunctionParseNode.BuiltInFunction(name = ProtoExistsFunction.NAME,
    nodeClass = ProtoExistsParseNode.class,
    args = { @FunctionParseNode.Argument(allowedTypes = { PProtobuf.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true), })
public class ProtoExistsFunction extends ScalarFunction {

  public static final String NAME = "PROTO_EXISTS";

  private volatile PathPlan cachedPlan;
  private final ProtoDescriptorRegistry registry;

  public ProtoExistsFunction() {
    this.registry = null;
  }

  public ProtoExistsFunction(List<Expression> children) {
    this(children, null);
  }

  /** @param registry the compiling connection's; {@code null} falls back to the per-JVM one */
  public ProtoExistsFunction(List<Expression> children, ProtoDescriptorRegistry registry) {
    super(children);
    this.registry = registry;
  }

  @Override
  public ProtoExistsFunction clone(List<Expression> children) {
    return new ProtoExistsFunction(children, registry);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType<?> getDataType() {
    return PBoolean.INSTANCE;
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
    // Walk the payload in place; ptr is overwritten only after the walk returns.
    byte[] buf = ptr.get();
    int offset = ptr.getOffset();
    int length = ptr.getLength();

    PathPlan plan;
    try {
      plan = ensurePlan();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    Object leaf;
    try {
      leaf = ProtoPathWalker.walk(buf, offset, length, plan);
    } catch (IOException e) {
      // Malformed payload (incl. index-compile dummy-UPSERT probe) → FALSE.
      ptr.set(PBoolean.INSTANCE.toBytes(false));
      return true;
    }
    boolean present = leaf != null && leaf != ProtoPathWalker.LEAF_ABSENT;
    ptr.set(PBoolean.INSTANCE.toBytes(present));
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
        ProtoDescriptorRegistry.PlanKind.PRESENCE, null, ProtoPathResolver::resolveForExistence);
      cachedPlan = plan;
      return plan;
    }
  }
}
