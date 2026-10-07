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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.util.proto.ProtoTagPathParser;
import org.apache.phoenix.expression.util.proto.ProtoTagPathWalker;
import org.apache.phoenix.expression.util.proto.TagPathPlan;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ProtoValueAtTagParseNode;
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

/**
 * {@code PROTO_VALUE_AT_TAG(payload, tagPath, sqlType[, default])} — extracts a leaf value from raw
 * protobuf wire bytes at the given tag-number path, decoded as the requested SQL type. Schema-less:
 * no descriptor registration required.
 */
@FunctionParseNode.BuiltInFunction(name = ProtoValueAtTagFunction.NAME,
    nodeClass = ProtoValueAtTagParseNode.class,
    args = { @FunctionParseNode.Argument(allowedTypes = { PProtobuf.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true,
          defaultValue = ProtoValueAtTagFunction.DEFAULT_VALUE), })
public class ProtoValueAtTagFunction extends ScalarFunction {

  public static final String NAME = "PROTO_VALUE_AT_TAG";

  static final String DEFAULT_VALUE = "null";

  private volatile TagPathPlan cachedPlan;

  public ProtoValueAtTagFunction() {
  }

  public ProtoValueAtTagFunction(List<Expression> children) {
    super(children);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType<?> getDataType() {
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

    TagPathPlan plan;
    try {
      plan = ensurePlan();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    Object leaf;
    try {
      leaf = ProtoTagPathWalker.walk(buf, offset, length, plan);
    } catch (IOException e) {
      return returnDefaultOrNull(ptr);
    }

    if (leaf == null) {
      return returnDefaultOrNull(ptr);
    }
    byte[] out = coerceLeaf(leaf, plan.getRequestedType());
    if (out == null) {
      // The value is present but does not fit the requested type, so the default must not apply.
      return ProtoFunctionsUtil.setNull(ptr);
    }
    ptr.set(out);
    return true;
  }

  private TagPathPlan ensurePlan() throws SQLException {
    TagPathPlan snapshot = cachedPlan;
    if (snapshot != null) {
      return snapshot;
    }
    synchronized (this) {
      snapshot = cachedPlan;
      if (snapshot != null) {
        return snapshot;
      }
      String pathLiteral =
        ProtoFunctionsUtil.requiredStringLiteral(getChildren().get(1), NAME, "tag-path");
      String typeName =
        ProtoFunctionsUtil.requiredStringLiteral(getChildren().get(2), NAME, "SQL type");
      PDataType<?> reqType = ProtoFunctionsUtil.resolveSqlType(typeName, NAME);
      int wireType = ProtoTagPathWalker.wireTypeForSqlType(reqType);
      if (wireType < 0) {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
          .setMessage("PROTO_VALUE_AT_TAG: unsupported SQL type '" + typeName + "'").build()
          .buildException();
      }
      List<ProtoTagPathParser.TagSegment> segments = ProtoTagPathParser.parse(pathLiteral);
      TagPathPlan plan = new TagPathPlan(Collections.unmodifiableList(segments), reqType, wireType);
      cachedPlan = plan;
      return plan;
    }
  }

  private boolean returnDefaultOrNull(ImmutableBytesWritable ptr) {
    return ProtoFunctionsUtil.returnDefaultOrNull(getChildren(), getChildren().size() > 3, ptr);
  }

  private static byte[] coerceLeaf(Object leaf, PDataType<?> requestedType) {
    if (leaf == null) {
      return null;
    }
    if (requestedType == PVarchar.INSTANCE) {
      if (leaf instanceof String) {
        return PVarchar.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof byte[]) {
        return PVarchar.INSTANCE.toBytes(Bytes.toString((byte[]) leaf));
      }
      return PVarchar.INSTANCE.toBytes(leaf.toString());
    }
    if (requestedType == PBoolean.INSTANCE && leaf instanceof Boolean) {
      return PBoolean.INSTANCE.toBytes(leaf);
    }
    if (requestedType == PInteger.INSTANCE) {
      if (leaf instanceof Integer) {
        return PInteger.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof Long) {
        long v = (Long) leaf;
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
          return null;
        }
        return PInteger.INSTANCE.toBytes((int) v);
      }
    }
    if (requestedType == PLong.INSTANCE) {
      if (leaf instanceof Long) {
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
    if (requestedType == PUnsignedLong.INSTANCE) {
      if (leaf instanceof Long) {
        long v = (Long) leaf;
        if (v < 0) {
          return null;
        }
        return PUnsignedLong.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof Integer) {
        return PUnsignedLong.INSTANCE.toBytes(((Integer) leaf).longValue());
      }
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
      if (leaf instanceof BigDecimal) {
        return PDecimal.INSTANCE.toBytes(leaf);
      }
      if (leaf instanceof Long) {
        return PDecimal.INSTANCE.toBytes(new BigDecimal((Long) leaf));
      }
      if (leaf instanceof Integer) {
        return PDecimal.INSTANCE.toBytes(new BigDecimal((Integer) leaf));
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
