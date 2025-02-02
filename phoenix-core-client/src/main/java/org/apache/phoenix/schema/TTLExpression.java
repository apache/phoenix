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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.CreateTableStatement;

public abstract class TTLExpression {

    public static final TTLExpression TTL_EXPRESSION_FOREVER =
            new LiteralTTLExpression(HConstants.FOREVER);
    public static final TTLExpression TTL_EXPRESSION_NOT_DEFINED =
            new LiteralTTLExpression(PhoenixDatabaseMetaData.TTL_NOT_DEFINED);

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

    public static TTLExpression create (int ttlValue) {
        if (ttlValue == PhoenixDatabaseMetaData.TTL_NOT_DEFINED) {
            return TTL_EXPRESSION_NOT_DEFINED;
        } else if (ttlValue == HConstants.FOREVER) {
            return TTL_EXPRESSION_FOREVER;
        } else {
            return new LiteralTTLExpression(ttlValue);
        }
    }

    public static TTLExpression create (TTLExpression ttlExpr) {
        if (ttlExpr instanceof LiteralTTLExpression) {
            return new LiteralTTLExpression((LiteralTTLExpression) ttlExpr);
        } else {
            return new ConditionalTTLExpression((ConditionalTTLExpression) ttlExpr);
        }
    }

    public static TTLExpression create(byte[] phoenixTTL) throws IOException {
        return createFromProto(PTableProtos.TTLExpression.parseFrom(phoenixTTL));
    }

    public static TTLExpression createFromProto(
            PTableProtos.TTLExpression ttlExpressionProto) throws IOException {
        if (ttlExpressionProto.hasLiteral()) {
                return LiteralTTLExpression.createFromProto(ttlExpressionProto.getLiteral());
        }
        if (ttlExpressionProto.hasCondition()) {
            return ConditionalTTLExpression.createFromProto(ttlExpressionProto.getCondition());
        }
        throw new RuntimeException("Unxexpected! Shouldn't reach here");
    }

    /**
     * Serialize the TTL expression as a protobuf byte[]
     * @param connection
     * @param table
     * @return protobuf for the TTL expression
     * @throws SQLException
     */
    public byte[] getTTLForScanAttribute(PhoenixConnection connection,
                                         PTable table) throws SQLException {
        try {
            PTableProtos.TTLExpression proto = toProto(connection, table);
            return proto != null ? proto.toByteArray() : null;
        } catch (IOException e) {
            throw new SQLException(
                    String.format("Error serializing %s as scan attribute", this), e);
        }
    }

    abstract public String getTTLExpression();

    /**
     * Returns the TTL value used for masking in TTLRegionScanner
     * @param result Input row
     * @return ttl value in seconds
     */
    abstract public long getRowTTLForMasking(List<Cell> result);

    /**
     * Returns the TTL value used during compaction in CompactionScanner
     * @param result Input row
     * @return ttl value in seconds
     */
    abstract public long getRowTTLForCompaction(List<Cell> result);

    abstract public String toString();

    abstract public void validateTTLOnCreate(PhoenixConnection conn,
                                             CreateTableStatement create,
                                             PTable parent,
                                             Map<String, Object> tableProps) throws SQLException;

    abstract public void validateTTLOnAlter(PhoenixConnection connection,
                                            PTable table) throws SQLException;

    abstract public TTLExpression compileTTLExpression(
            PhoenixConnection connection, PTable table) throws SQLException;

    abstract public PTableProtos.TTLExpression toProto(
            PhoenixConnection connection, PTable table) throws SQLException, IOException;
}
