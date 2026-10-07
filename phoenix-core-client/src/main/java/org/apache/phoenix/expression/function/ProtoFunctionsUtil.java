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

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.util.proto.PathPlan;
import org.apache.phoenix.expression.util.proto.ProtoPathParser;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistry;
import org.apache.phoenix.protobuf.descriptor.ProtoDescriptorRegistryHolder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;

import org.apache.phoenix.thirdparty.com.google.protobuf.Descriptors;

/**
 * Shared helpers for the PROTO_* SQL functions and their parse nodes.
 */
public final class ProtoFunctionsUtil {

  private ProtoFunctionsUtil() {
  }

  /** Resolves a parsed name path against a descriptor into a {@link PathPlan}. */
  @FunctionalInterface
  interface PathResolver {
    PathPlan resolve(List<ProtoPathParser.ParsedSegment> parsed, Descriptors.Descriptor descriptor,
      String pathLiteral) throws SQLException;
  }

  /**
   * Returns the value of {@code child} as a string, or throws if it is not a non-null literal.
   * {@code label} names the argument in the error message.
   */
  public static String requiredStringLiteral(Expression child, String functionName, String label)
    throws SQLException {
    if (!(child instanceof LiteralExpression)) {
      throw typeMismatch(functionName + " " + label + " must be a string literal");
    }
    Object value = ((LiteralExpression) child).getValue();
    if (value == null) {
      throw typeMismatch(functionName + " " + label + " must be non-null");
    }
    return value.toString();
  }

  /** Looks up a registered descriptor, or throws if it is not registered. */
  public static ProtoDescriptorRegistry.CachedDescriptor
    lookupDescriptor(ProtoDescriptorRegistry registry, String descriptorName, String functionName)
      throws SQLException {
    ProtoDescriptorRegistry.CachedDescriptor cached = registry.lookup(descriptorName);
    if (cached == null) {
      throw new SQLExceptionInfo.Builder(
        SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE)
          .setMessage(functionName + ": descriptor '" + descriptorName
            + "' not registered in SYSTEM.PROTO_DESCRIPTORS")
          .build().buildException();
    }
    return cached;
  }

  /** Returns {@code children} with the descriptor name appended as a VARCHAR literal. */
  public static List<Expression> withDescriptorName(List<Expression> children,
    String descriptorName) throws SQLException {
    List<Expression> augmented = new ArrayList<>(children.size() + 1);
    augmented.addAll(children);
    augmented.add(LiteralExpression.newConstant(descriptorName, PVarchar.INSTANCE));
    return augmented;
  }

  /**
   * Compiles (or fetches from the registry's plan cache) the name-path plan for a function whose
   * children are {@code (payload, path, ..., descriptorName)}.
   */
  static PathPlan compilePathPlan(List<Expression> children, String functionName,
    ProtoDescriptorRegistry registry, ProtoDescriptorRegistry.PlanKind kind,
    PDataType<?> requestedType, PathResolver resolver) throws SQLException {
    String descriptorName =
      requiredStringLiteral(children.get(children.size() - 1), functionName, "descriptor name");
    String pathLiteral = requiredStringLiteral(children.get(1), functionName, "path");
    return withDescriptorRefreshOnMiss(
      registry != null ? registry : ProtoDescriptorRegistryHolder.get(), descriptorName,
      functionName,
      cached -> cached.getOrCompilePathPlan(pathLiteral, kind, requestedType, descriptor -> resolver
        .resolve(ProtoPathParser.parse(pathLiteral), descriptor, pathLiteral)));
  }

  /** Work done against a looked-up descriptor; see {@link #withDescriptorRefreshOnMiss}. */
  @FunctionalInterface
  public interface DescriptorAction<T> {
    T apply(ProtoDescriptorRegistry.CachedDescriptor cached) throws SQLException;
  }

  /**
   * Looks up {@code descriptorName} and runs {@code action} against it. If that fails in a way a
   * newer descriptor could fix (see {@link #isPossiblyStaleDescriptorError}), forces one reload
   * from {@code SYSTEM.PROTO_DESCRIPTORS} and retries once.
   * <p>
   * The cached descriptor may predate a CREATE/ALTER run by another client and not have reached its
   * TTL yet. Protobuf Descriptor Evolution is add-only, so one reload resolves any legitimately new
   * field. The registry rate-limits forced reloads per name, so a genuinely bad path does not
   * reload the descriptor on every attempt.
   */
  public static <T> T withDescriptorRefreshOnMiss(ProtoDescriptorRegistry registry,
    String descriptorName, String functionName, DescriptorAction<T> action) throws SQLException {
    try {
      return action.apply(lookupDescriptor(registry, descriptorName, functionName));
    } catch (SQLException e) {
      if (!isPossiblyStaleDescriptorError(e) || !registry.tryForceRefresh(descriptorName)) {
        throw e;
      }
      return action.apply(lookupDescriptor(registry, descriptorName, functionName));
    }
  }

  static boolean isPossiblyStaleDescriptorError(SQLException e) {
    int code = e.getErrorCode();
    return code == SQLExceptionCode.PROTOBUF_PATH_FIELD_NOT_FOUND.getErrorCode()
      || code == SQLExceptionCode.PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE.getErrorCode();
  }

  /**
   * Evaluates the payload argument into {@code ptr}. Returns false when it cannot be evaluated yet
   * or is empty (the function then returns false). On true, the caller walks the payload in place
   * via {@code ptr.get()/getOffset()/getLength()} and must read those before writing its own result
   * into {@code ptr}.
   */
  static boolean evaluatePayload(Expression payloadArg, Tuple tuple, ImmutableBytesWritable ptr) {
    return payloadArg.evaluate(tuple, ptr) && ptr != null && ptr.getLength() != 0;
  }

  /**
   * Writes the user-supplied default (argument 3) into {@code ptr}, converted to the requested type
   * at compile time by {@link #withCompiledDefault}, or SQL NULL when there is no default.
   */
  static boolean returnDefaultOrNull(List<Expression> children, boolean hasDefaultArg,
    ImmutableBytesWritable ptr) {
    if (!hasDefaultArg) {
      return setNull(ptr);
    }
    children.get(3).evaluate(null, ptr);
    return true;
  }

  static boolean setNull(ImmutableBytesWritable ptr) {
    ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
    return true;
  }

  /** Resolves a requested SQL type name, or throws if it is unknown. */
  public static PDataType<?> resolveSqlType(String typeName, String functionName)
    throws SQLException {
    PDataType<?> type;
    try {
      type = PDataType.fromSqlTypeName(typeName);
    } catch (RuntimeException e) {
      type = null;
    }
    if (type == null) {
      throw typeMismatch(functionName + ": unknown SQL type '" + typeName + "'");
    }
    return type;
  }

  /**
   * Returns the first three of {@code children} plus, when {@code userSuppliedDefault} is true, the
   * default (argument 3) converted to the requested SQL type (argument 2). A {@code null} or
   * {@code 'null'} default becomes a typed null literal.
   * @throws SQLException if the default is not a literal or cannot be read as the requested type
   */
  public static List<Expression> withCompiledDefault(List<Expression> children,
    boolean userSuppliedDefault, String functionName) throws SQLException {
    List<Expression> compiled = new ArrayList<>(children.subList(0, 3));
    if (!userSuppliedDefault) {
      return compiled;
    }
    PDataType<?> requestedType = resolveSqlType(
      requiredStringLiteral(children.get(2), functionName, "SQL type"), functionName);
    Expression defaultChild = children.get(3);
    if (!(defaultChild instanceof LiteralExpression)) {
      throw typeMismatch(functionName + " default must be a string literal");
    }
    Object value = ((LiteralExpression) defaultChild).getValue();
    String s = value == null ? null : value.toString();
    Object converted =
      s == null || "null".equalsIgnoreCase(s) ? null : parseDefault(s, requestedType, functionName);
    compiled.add(LiteralExpression.newConstant(converted, requestedType));
    return compiled;
  }

  private static Object parseDefault(String s, PDataType<?> requestedType, String functionName)
    throws SQLException {
    if (requestedType == PVarchar.INSTANCE) {
      return s;
    }
    if (requestedType == PVarbinary.INSTANCE || requestedType == PVarbinaryEncoded.INSTANCE) {
      return s.getBytes(StandardCharsets.UTF_8);
    }
    if (requestedType == PBoolean.INSTANCE) {
      if ("true".equalsIgnoreCase(s)) {
        return Boolean.TRUE;
      }
      if ("false".equalsIgnoreCase(s)) {
        return Boolean.FALSE;
      }
      throw badDefault(s, requestedType, functionName);
    }
    if (
      requestedType == PInteger.INSTANCE || requestedType == PLong.INSTANCE
        || requestedType == PUnsignedInt.INSTANCE || requestedType == PUnsignedLong.INSTANCE
        || requestedType == PFloat.INSTANCE || requestedType == PDouble.INSTANCE
        || requestedType == PDecimal.INSTANCE
    ) {
      try {
        return requestedType.toObject(s);
      } catch (RuntimeException e) {
        throw badDefault(s, requestedType, functionName);
      }
    }
    throw typeMismatch(
      functionName + " does not support a default for SQL type " + requestedType.getSqlTypeName());
  }

  private static SQLException badDefault(String s, PDataType<?> requestedType,
    String functionName) {
    return typeMismatch(functionName + " default '" + s + "' is not a valid "
      + requestedType.getSqlTypeName() + " value");
  }

  private static SQLException typeMismatch(String message) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
      .setMessage(message).build().buildException();
  }
}
