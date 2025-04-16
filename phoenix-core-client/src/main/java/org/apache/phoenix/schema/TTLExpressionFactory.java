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
package org.apache.phoenix.schema;

import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_FOREVER;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

public class TTLExpressionFactory {

  public static TTLExpression create(String ttlExpr) {
    if (PhoenixDatabaseMetaData.NONE_TTL.equalsIgnoreCase(ttlExpr)) {
      return TTL_EXPRESSION_NOT_DEFINED;
    } else if (PhoenixDatabaseMetaData.FOREVER_TTL.equalsIgnoreCase(ttlExpr)) {
      return TTL_EXPRESSION_FOREVER;
    } else {
      try {
        int ttlValue = Integer.parseInt(ttlExpr);
        return create(ttlValue);
      } catch (NumberFormatException e) {
        return new ConditionalTTLExpression(ttlExpr);
      }
    }
  }

  public static LiteralTTLExpression create(int ttlValue) {
    if (ttlValue == PhoenixDatabaseMetaData.TTL_NOT_DEFINED) {
      return TTL_EXPRESSION_NOT_DEFINED;
    } else if (ttlValue == HConstants.FOREVER) {
      return TTL_EXPRESSION_FOREVER;
    } else {
      return new LiteralTTLExpression(ttlValue);
    }
  }

  public static TTLExpression create(TTLExpression ttlExpr) {
    if (ttlExpr instanceof LiteralTTLExpression) {
      return new LiteralTTLExpression((LiteralTTLExpression) ttlExpr);
    } else {
      return new ConditionalTTLExpression((ConditionalTTLExpression) ttlExpr);
    }
  }

  public static CompiledTTLExpression create(byte[] phoenixTTL) throws IOException {
    return createFromProto(PTableProtos.TTLExpression.parseFrom(phoenixTTL));
  }

  public static CompiledTTLExpression createFromProto(PTableProtos.TTLExpression ttlExpressionProto)
    throws IOException {
    if (ttlExpressionProto.hasLiteral()) {
      return LiteralTTLExpression.createFromProto(ttlExpressionProto.getLiteral());
    }
    if (ttlExpressionProto.hasCondition()) {
      return CompiledConditionalTTLExpression.createFromProto(ttlExpressionProto.getCondition());
    }
    throw new RuntimeException("Unxexpected! Shouldn't reach here");
  }
}
