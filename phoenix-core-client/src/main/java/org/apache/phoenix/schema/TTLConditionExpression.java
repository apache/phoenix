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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_TTL;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;

public class TTLConditionExpression extends TTLExpression {
    private final String ttlExpr;

    public TTLConditionExpression(String ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TTLConditionExpression that = (TTLConditionExpression) o;
        return ttlExpr.equals(that.ttlExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttlExpr);
    }

    @Override
    public String getTTLExpression() {
        return ttlExpr;
    }

    @Override
    public String toString() {
        return getTTLExpression();
    }

    @Override
    /**
     * @param result row to be evaluated against the conditional ttl expression
     * @return DEFAULT_TTL (FOREVER) if the expression evaluates to False else 0
     * if the expression evaluates to true i.e. row is expired
     */
    public long getTTLForRow(List<Cell> result) {
        // TODO
        return DEFAULT_TTL;
    }

    @Override
    public void validateTTLOnCreation(PhoenixConnection conn, CreateTableStatement create) throws SQLException {
        ParseNode ttlCondition = SQLParser.parseCondition(this.ttlExpr);
        VerifyCreateConditionalTTLExpression condTTLVisitor = new VerifyCreateConditionalTTLExpression(conn, create);
        Expression ttlExpression = ttlCondition.accept(condTTLVisitor);
        if (ttlExpression.getDataType() != PBoolean.INSTANCE) {
            throw TypeMismatchException.newException(PBoolean.INSTANCE,
                    ttlExpression.getDataType(), ttlExpression.toString());
        }
    }

    @Override
    public void validateTTLOnAlter(PhoenixConnection conn, PTable table) throws SQLException {
        ParseNode ttlCondition = SQLParser.parseCondition(this.ttlExpr);
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(table));
        StatementContext context = new StatementContext(new PhoenixStatement(conn), resolver);
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        Expression ttlExpression = ttlCondition.accept(expressionCompiler);
        if (ttlExpression.getDataType() != PBoolean.INSTANCE) {
            throw TypeMismatchException.newException(PBoolean.INSTANCE,
                    ttlExpression.getDataType(), ttlExpression.toString());
        }
    }

    @Override
    public String getTTLForScanAttribute() {
        // Conditional TTL is not sent as a scan attribute
        // Masking is implemented using query re-write
        return null;
    }

    /**
     * Validates that all the columns used in the conditional TTL expression are present in the table
     * or its parent table in case of view
     */
    private static class VerifyCreateConditionalTTLExpression extends ExpressionCompiler {
        private final CreateTableStatement create;
        private final ColumnResolver baseTableResolver;

        private VerifyCreateConditionalTTLExpression(PhoenixConnection conn,
                                                   CreateTableStatement create) throws SQLException {
            super(new StatementContext(new PhoenixStatement(conn)));
            this.create = create;
            // Returns the resolver for base table if base table is not null (in case of views)
            // Else, returns FromCompiler#EMPTY_TABLE_RESOLVER which is a no-op resolver
            this.baseTableResolver = FromCompiler.getResolverForCreation(create, conn);
        }

        @Override
        public Expression visit(ColumnParseNode node) throws SQLException {
            // First check current table
            for (ColumnDef columnDef : create.getColumnDefs()) {
                ColumnName columnName = columnDef.getColumnDefName();
                // Takes family name into account
                if (columnName.toString().equals(node.getFullName())) {
                    String cf = columnName.getFamilyName();
                    String cq = columnName.getColumnName();
                    return new KeyValueColumnExpression( new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return columnDef.isNull();
                        }
                        @Override
                        public PDataType getDataType() {
                            return columnDef.getDataType();
                        }
                        @Override
                        public Integer getMaxLength() {
                            return columnDef.getMaxLength();
                        }
                        @Override
                        public Integer getScale() {
                            return columnDef.getScale();
                        }
                        @Override
                        public SortOrder getSortOrder() {
                            return columnDef.getSortOrder();
                        }
                    }, cf != null ? Bytes.toBytes(cf) : null, Bytes.toBytes(cq));
                }
            }
            // Column used in TTL expression not found in current, check the parent
            ColumnRef columnRef = baseTableResolver.resolveColumn(
                    node.getSchemaName(), node.getTableName(), node.getName());
            return columnRef.newColumnExpression(node.isTableNameCaseSensitive(), node.isCaseSensitive());
        }
    }
}
