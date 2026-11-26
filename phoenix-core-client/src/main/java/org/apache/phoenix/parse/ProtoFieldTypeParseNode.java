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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.ProtoFieldTypeFunction;
import org.apache.phoenix.expression.function.ProtoFunctionsUtil;
import org.apache.phoenix.expression.util.proto.PathPlan;
import org.apache.phoenix.expression.util.proto.ProtoPathParser;
import org.apache.phoenix.expression.util.proto.ProtoPathResolver;

/**
 * Parse node for {@code PROTO_FIELD_TYPE(payload, path)}.
 */
public class ProtoFieldTypeParseNode extends FunctionParseNode {

  public ProtoFieldTypeParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
    super(name, children, info);
  }

  @Override
  public FunctionExpression create(List<Expression> children, StatementContext context)
    throws SQLException {
    String descriptorName = ProtoValueParseNode.resolveDescriptorNameForPayload(children.get(0),
      context, "PROTO_FIELD_TYPE");
    String pathLiteral = ProtoFunctionsUtil.requiredStringLiteral(children.get(1),
      ProtoFieldTypeFunction.NAME, "path");
    String resolvedTypeName = resolveTypeName(context, descriptorName, pathLiteral);
    return new ProtoFieldTypeFunction(
      ProtoFunctionsUtil.withDescriptorName(children, resolvedTypeName));
  }

  private static String resolveTypeName(StatementContext context, String descriptorName,
    String pathLiteral) throws SQLException {
    List<ProtoPathParser.ParsedSegment> parsed = ProtoPathParser.parse(pathLiteral);
    PathPlan plan = ProtoFunctionsUtil.withDescriptorRefreshOnMiss(
      context.getConnection().getProtoDescriptorRegistry(), descriptorName,
      ProtoFieldTypeFunction.NAME,
      cached -> ProtoPathResolver.resolveForExistence(parsed, cached.getDescriptor(), pathLiteral));
    return ProtoPathResolver.canonicalSqlTypeName(plan.getLeafProtoFieldType());
  }
}
