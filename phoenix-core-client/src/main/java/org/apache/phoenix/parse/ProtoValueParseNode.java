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
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.ProtoFunctionsUtil;
import org.apache.phoenix.expression.function.ProtoValueFunction;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PProtobuf;

/**
 * Parse node for the {@code PROTO_VALUE(payload, path, type[, default])} function.
 */
public class ProtoValueParseNode extends FunctionParseNode {

  public ProtoValueParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
    super(name, children, info);
  }

  @Override
  public FunctionExpression create(List<Expression> children, StatementContext context)
    throws SQLException {
    String descriptorName =
      resolveDescriptorNameForPayload(children.get(0), context, "PROTO_VALUE");
    // validate() pads an omitted default; keep it only if the query supplied one.
    List<Expression> compiled = ProtoFunctionsUtil.withCompiledDefault(children,
      getChildren().size() > 3, ProtoValueFunction.NAME);
    ProtoValueFunction function =
      new ProtoValueFunction(ProtoFunctionsUtil.withDescriptorName(compiled, descriptorName),
        context.getConnection().getProtoDescriptorRegistry());
    function.ensurePlan();
    return function;
  }

  public static String resolveDescriptorNameForPayload(Expression payload, StatementContext context,
    String functionName) throws SQLException {
    Expression unwrapped = payload;
    while (unwrapped instanceof CoerceExpression) {
      unwrapped = ((CoerceExpression) unwrapped).getChild();
    }
    if (!(unwrapped instanceof ColumnExpression)) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
        .setMessage(functionName + " requires the first argument to be a direct "
          + "reference to a PROTOBUF<...>-typed column; expression-typed payloads "
          + "and cast chains are not yet supported.")
        .build().buildException();
    }
    PColumn payloadColumn = resolveColumn(context.getResolver(), unwrapped);
    if (payloadColumn == null) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
        .setMessage(functionName + " could not resolve the payload column '" + unwrapped
          + "' against any table in scope")
        .build().buildException();
    }
    if (
      !(payloadColumn.getDataType() instanceof PProtobuf)
        && payloadColumn.getProtoMessageName() == null
    ) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
        .setMessage(functionName + " requires the payload column to be typed "
          + "PROTOBUF<...>; column '" + payloadColumn.getName().getString() + "' is typed "
          + payloadColumn.getDataType().getSqlTypeName())
        .build().buildException();
    }
    String descriptorName = payloadColumn.getProtoMessageName();
    if (descriptorName == null || descriptorName.isEmpty()) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
        .setMessage(functionName + " payload column '" + payloadColumn.getName().getString()
          + "' has no attached PROTOBUF descriptor name — the SYSTEM.CATALOG entry "
          + "may be corrupt or missing PROTOBUF_DESCRIPTOR_NAME")
        .build().buildException();
    }
    return descriptorName;
  }

  /**
   * Returns the source {@link PColumn} a projected expression reads from, looking through
   * coercions, or null if the expression is not a direct column reference.
   */
  public static PColumn resolveSourceColumn(ColumnResolver resolver, Expression expression) {
    Expression unwrapped = expression;
    while (unwrapped instanceof CoerceExpression) {
      unwrapped = ((CoerceExpression) unwrapped).getChild();
    }
    if (unwrapped.getDataType() != PProtobuf.INSTANCE) {
      return null;
    }
    return resolveColumn(resolver, unwrapped);
  }

  private static PColumn resolveColumn(ColumnResolver resolver, Expression unwrapped) {
    if (unwrapped instanceof ProjectedColumnExpression) {
      return ((ProjectedColumnExpression) unwrapped).getColumn();
    }
    if (unwrapped instanceof KeyValueColumnExpression) {
      return findColumn(resolver, (KeyValueColumnExpression) unwrapped);
    }
    return null;
  }

  private static PColumn findColumn(ColumnResolver resolver, KeyValueColumnExpression kv) {
    if (resolver == null) {
      return null;
    }
    byte[] cf = kv.getColumnFamily();
    // Single-cell storage shares one HBase qualifier across columns; the column's own qualifier is
    // its position in the cell.
    byte[] cq = kv instanceof SingleCellColumnExpression
      ? ((SingleCellColumnExpression) kv).getPositionInArray()
      : kv.getColumnQualifier();
    for (TableRef tableRef : resolver.getTables()) {
      PTable table = tableRef.getTable();
      for (PColumn column : table.getColumns()) {
        if (column.getFamilyName() == null) {
          continue;
        }
        if (!Bytes.equals(column.getFamilyName().getBytes(), cf)) {
          continue;
        }
        byte[] columnQualifier = column.getColumnQualifierBytes() != null
          ? column.getColumnQualifierBytes()
          : column.getName().getBytes();
        if (Arrays.equals(columnQualifier, cq)) {
          return column;
        }
      }
    }
    return null;
  }
}
