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
import java.util.List;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.ProtoFunctionsUtil;
import org.apache.phoenix.expression.function.ProtoValueAtTagFunction;
import org.apache.phoenix.expression.util.proto.ProtoTagPathParser;
import org.apache.phoenix.expression.util.proto.ProtoTagPathWalker;
import org.apache.phoenix.schema.types.PDataType;

/**
 * Parse node for {@code PROTO_VALUE_AT_TAG(payload, tagPath, sqlType[, default])}. Validates the
 * tag-path grammar and SQL type at compile time. Does not require a descriptor — works on both
 * {@code PROTOBUF<...>} and plain {@code VARBINARY} columns.
 */
public class ProtoValueAtTagParseNode extends FunctionParseNode {

  public ProtoValueAtTagParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
    super(name, children, info);
  }

  @Override
  public FunctionExpression create(List<Expression> children, StatementContext context)
    throws SQLException {
    String pathLiteral = ProtoFunctionsUtil.requiredStringLiteral(children.get(1),
      ProtoValueAtTagFunction.NAME, "tag-path");
    String typeName = ProtoFunctionsUtil.requiredStringLiteral(children.get(2),
      ProtoValueAtTagFunction.NAME, "SQL type");

    ProtoTagPathParser.parse(pathLiteral);
    PDataType<?> sqlType =
      ProtoFunctionsUtil.resolveSqlType(typeName, ProtoValueAtTagFunction.NAME);
    int wireType = ProtoTagPathWalker.wireTypeForSqlType(sqlType);
    if (wireType < 0) {
      throw new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_VALUE_TYPE_MISMATCH)
        .setMessage("PROTO_VALUE_AT_TAG: unsupported SQL type '" + typeName + "'").build()
        .buildException();
    }

    // validate() pads an omitted default; keep it only if the query supplied one.
    return new ProtoValueAtTagFunction(ProtoFunctionsUtil.withCompiledDefault(children,
      getChildren().size() > 3, ProtoValueAtTagFunction.NAME));
  }
}
