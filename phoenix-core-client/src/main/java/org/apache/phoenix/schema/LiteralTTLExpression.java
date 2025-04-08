/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class LiteralTTLExpression implements TTLExpression, CompiledTTLExpression {
    private final int ttlValue;

    public static final LiteralTTLExpression TTL_EXPRESSION_FOREVER =
            new LiteralTTLExpression(HConstants.FOREVER);
    public static final LiteralTTLExpression TTL_EXPRESSION_NOT_DEFINED =
            new LiteralTTLExpression(PhoenixDatabaseMetaData.TTL_NOT_DEFINED);

    public LiteralTTLExpression(int ttl) {
        Preconditions.checkArgument(ttl >= 0);
        this.ttlValue = ttl;
    }

    public LiteralTTLExpression(LiteralTTLExpression ttlExpr) {
        this.ttlValue = ttlExpr.ttlValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiteralTTLExpression that = (LiteralTTLExpression) o;
        return ttlValue == that.ttlValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttlValue);
    }

    @Override
    public String getTTLExpression() {
        return String.valueOf(ttlValue);
    }

    @Override
    public String toString() {
        return getTTLExpression();
    }

    @Override
    public long getRowTTLForMasking(List<Cell> result, boolean isRaw) {
        return getRowTTLForCompaction(result);
    }

    @Override
    public long getRowTTLForCompaction(List<Cell> result) {
        return ttlValue;
    }

    @Override
    public void validateTTLOnCreate(PhoenixConnection conn,
                                    CreateTableStatement create,
                                    PTable parent,
                                    Map<String, Object> tableProps) {

    }

    @Override
    public void validateTTLOnAlter(PhoenixConnection connection, PTable table) {}

    @Override
    public CompiledTTLExpression compileTTLExpression(PhoenixConnection connection, PTable table) {
        return this;
    }

    public static LiteralTTLExpression createFromProto(PTableProtos.LiteralTTL literal) {
        return new LiteralTTLExpression(literal.getTtlValue());
    }

    @Override
    public PTableProtos.TTLExpression toProto() throws SQLException {
        if (this.equals(TTL_EXPRESSION_NOT_DEFINED)) {
            return null;
        }
        PTableProtos.TTLExpression.Builder ttl = PTableProtos.TTLExpression.newBuilder();
        PTableProtos.LiteralTTL.Builder literal = PTableProtos.LiteralTTL.newBuilder();
        literal.setTtlValue(ttlValue);
        ttl.setLiteral(literal.build());
        return ttl.build();
    }

    public int getTTLValue() {
        return ttlValue;
    }
}
