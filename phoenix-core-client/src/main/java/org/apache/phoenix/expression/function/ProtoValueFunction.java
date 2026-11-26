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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.util.proto.PathPlan;
import org.apache.phoenix.expression.util.proto.ProtoPathResolver;
import org.apache.phoenix.expression.util.proto.ProtoPathWalker;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ProtoValueParseNode;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * {@code PROTO_VALUE(payload, path, type[, default])} — extracts a leaf value from a PROTOBUF-typed
 * column at the given path, coerced to the requested SQL type.
 * <p>
 * The optional default applies only when the payload is well-formed but the leaf is absent (unset
 * field, missing parent, map key or index). SQL NULL is returned, regardless of the default, when
 * the payload is malformed or the value does not fit the requested type. Malformed-payload
 * detection is best-effort: only the bytes scanned up to the leaf are validated.
 */
@FunctionParseNode.BuiltInFunction(name = ProtoValueFunction.NAME,
    nodeClass = ProtoValueParseNode.class,
    args = { @FunctionParseNode.Argument(allowedTypes = { PProtobuf.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true,
          defaultValue = ProtoValueFunction.DEFAULT_VALUE), })
public class ProtoValueFunction extends ScalarFunction {

  public static final String NAME = "PROTO_VALUE";

  static final String DEFAULT_VALUE = "null";

  private volatile PathPlan cachedPlan;
  private final ProtoDescriptorRegistry registry;

  public ProtoValueFunction() {
    this.registry = null;
  }

  public ProtoValueFunction(List<Expression> children) {
    this(children, null);
  }

  /** @param registry the compiling connection's; {@code null} falls back to the per-JVM one */
  public ProtoValueFunction(List<Expression> children, ProtoDescriptorRegistry registry) {
    super(children);
    this.registry = registry;
  }

  @Override
  public ProtoValueFunction clone(List<Expression> children) {
    return new ProtoValueFunction(children, registry);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isNullable() {
    return true;
  }

  @Override
  public PDataType<?> getDataType() {
    return getPDataType();
  }

  private PDataType<?> getPDataType() {
    String dataType = (String) ((LiteralExpression) getChildren().get(2)).getValue();
    return PDataType.fromSqlTypeName(dataType);
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
      return ProtoFunctionsUtil.setNull(ptr);
    }
    if (leaf == ProtoPathWalker.LEAF_ABSENT) {
      // Parent present, leaf unset: the user default wins, else the field's own default.
      if (hasDefaultArg() || plan.getLeafDefault() == null) {
        return returnDefaultOrNull(ptr);
      }
      leaf = plan.getLeafDefault();
    }
    // A null leaf means the parent, message, map key or index is missing.
    if (leaf == null) {
      return returnDefaultOrNull(ptr);
    }
    byte[] out = coerceLeaf(leaf, plan);
    if (out == null) {
      // The value is present but does not fit the requested type, so the default must not apply.
      return ProtoFunctionsUtil.setNull(ptr);
    }
    ptr.set(out);
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
      String requestedTypeName =
        ProtoFunctionsUtil.requiredStringLiteral(getChildren().get(2), NAME, "requested SQL type");
      PDataType<?> requestedType = ProtoFunctionsUtil.resolveSqlType(requestedTypeName, NAME);
      PathPlan plan = ProtoFunctionsUtil.compilePathPlan(getChildren(), NAME, registry,
        ProtoDescriptorRegistry.PlanKind.VALUE, requestedType, (parsed, descriptor,
          path) -> ProtoPathResolver.resolve(parsed, descriptor, requestedType, path));
      cachedPlan = plan;
      return plan;
    }
  }

  /** Children are (payload, path, type[, default], descriptorName). */
  private boolean hasDefaultArg() {
    return getChildren().size() > 4;
  }

  private boolean returnDefaultOrNull(ImmutableBytesWritable ptr) {
    return ProtoFunctionsUtil.returnDefaultOrNull(getChildren(), hasDefaultArg(), ptr);
  }

  private static boolean isUnsigned64(FieldDescriptor.Type leafType) {
    return leafType == FieldDescriptor.Type.UINT64 || leafType == FieldDescriptor.Type.FIXED64;
  }

  /**
   * Encodes {@code v} as DECIMAL, or returns null when the encoding cannot represent it.
   */
  private static byte[] decimalBytesOrNull(BigDecimal v) {
    byte[] bytes = PDecimal.INSTANCE.toBytes(v);
    BigDecimal decoded = (BigDecimal) PDecimal.INSTANCE.toObject(bytes, 0, bytes.length);
    return v.compareTo(decoded) == 0 ? bytes : null;
  }

  private static byte[] coerceLeaf(Object leaf, PathPlan plan) {
    PDataType<?> requestedType = plan.getRequestedType();
    FieldDescriptor.Type leafType = plan.getLeafProtoFieldType();
    if (requestedType == PVarchar.INSTANCE) {
      if (leaf instanceof String) {
        return PVarchar.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof byte[]) {
        return (byte[]) leaf;
      }
      return PVarchar.INSTANCE.toBytes(leaf.toString());
    }
    if (requestedType == PBoolean.INSTANCE && leaf instanceof Boolean) {
      return PBoolean.INSTANCE.toBytes(leaf);
    }
    if (requestedType == PInteger.INSTANCE && leaf instanceof Integer) {
      return PInteger.INSTANCE.toBytes(leaf);
    }
    if (requestedType == PInteger.INSTANCE && leaf instanceof Long) {
      long v = (Long) leaf;
      if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
        return null;
      }
      return PInteger.INSTANCE.toBytes((int) v);
    }
    if (requestedType == PLong.INSTANCE) {
      if (leaf instanceof Long) {
        // An unsigned 64-bit value of 2^63 or more does not fit in BIGINT.
        if ((Long) leaf < 0 && isUnsigned64(leafType)) {
          return null;
        }
        return PLong.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof Integer) {
        return PLong.INSTANCE.toBytes(((Integer) leaf).longValue());
      }
    }
    if (requestedType == PUnsignedInt.INSTANCE) {
      if (leaf instanceof Integer) {
        return PUnsignedInt.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof Long) {
        long v = (Long) leaf;
        if (v < 0 || v > Integer.MAX_VALUE) {
          return null;
        }
        return PUnsignedInt.INSTANCE.toBytes((int) v);
      }
    }
    if (requestedType == PUnsignedLong.INSTANCE && leaf instanceof Long) {
      long v = (Long) leaf;
      if (v < 0) {
        return null;
      }
      return PUnsignedLong.INSTANCE.toBytes(leaf);
    }
    if (requestedType == PFloat.INSTANCE && leaf instanceof Float) {
      return PFloat.INSTANCE.toBytes(leaf);
    }
    if (requestedType == PDouble.INSTANCE) {
      if (leaf instanceof Double) {
        return PDouble.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof Float) {
        return PDouble.INSTANCE.toBytes(((Float) leaf).doubleValue());
      }
    }
    if (requestedType == PDecimal.INSTANCE) {
      if (leaf instanceof Integer) {
        return PDecimal.INSTANCE.toBytes(new BigDecimal((Integer) leaf));
      }
      if (leaf instanceof Long) {
        long v = (Long) leaf;
        return PDecimal.INSTANCE.toBytes(isUnsigned64(leafType)
          ? new BigDecimal(Long.toUnsignedString(v))
          : BigDecimal.valueOf(v));
      }
      if (leaf instanceof Float) {
        float v = (Float) leaf;
        return Float.isFinite(v)
          ? PDecimal.INSTANCE.toBytes(new BigDecimal(Float.toString(v)))
          : null;
      }
      if (leaf instanceof Double) {
        double v = (Double) leaf;
        return Double.isFinite(v) ? decimalBytesOrNull(BigDecimal.valueOf(v)) : null;
      }
    }
    if (requestedType == PVarbinary.INSTANCE) {
      if (leaf instanceof byte[]) {
        return (byte[]) leaf;
      }
      if (leaf instanceof String) {
        return PVarchar.INSTANCE.toBytes(leaf);
      }
    }
    if (requestedType == PVarbinaryEncoded.INSTANCE) {
      if (leaf instanceof byte[]) {
        return PVarbinaryEncoded.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof String) {
        return PVarbinaryEncoded.INSTANCE.toBytes(((String) leaf).getBytes(StandardCharsets.UTF_8));
      }
    }
    return null;
  }
}
