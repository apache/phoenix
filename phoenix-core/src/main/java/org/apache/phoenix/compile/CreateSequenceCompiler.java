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
package org.apache.phoenix.compile;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Collections;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.SortOrder;


public class CreateSequenceCompiler {
    private final PhoenixStatement statement;

    public CreateSequenceCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    private static class LongDatum implements PDatum {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.LONG;
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
        
    }
    private static class IntegerDatum implements PDatum {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.INTEGER;
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }
        
    }
    private static final PDatum LONG_DATUM = new LongDatum();
    private static final PDatum INTEGER_DATUM = new IntegerDatum();

    public MutationPlan compile(final CreateSequenceStatement sequence) throws SQLException {
        ParseNode startsWithNode = sequence.getStartWith();
        ParseNode incrementByNode = sequence.getIncrementBy();
        if (!startsWithNode.isStateless()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.STARTS_WITH_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        if (!incrementByNode.isStateless()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        ParseNode cacheNode = sequence.getCacheSize();
        if (cacheNode != null && !cacheNode.isStateless()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        
        final PhoenixConnection connection = statement.getConnection();
        
        final StatementContext context = new StatementContext(statement);
        if (startsWithNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)startsWithNode, LONG_DATUM);
        }
        if (incrementByNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)incrementByNode, LONG_DATUM);
        }
        if (cacheNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)cacheNode, INTEGER_DATUM);
        }
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        Expression startsWithExpr = startsWithNode.accept(expressionCompiler);
        ImmutableBytesWritable ptr = context.getTempPtr();
        startsWithExpr.evaluate(null, ptr);
        if (ptr.getLength() == 0 || !startsWithExpr.getDataType().isCoercibleTo(PDataType.LONG)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.STARTS_WITH_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        final long startsWith = (Long)PDataType.LONG.toObject(ptr, startsWithExpr.getDataType());

        Expression incrementByExpr = incrementByNode.accept(expressionCompiler);
        incrementByExpr.evaluate(null, ptr);
        if (ptr.getLength() == 0 || !incrementByExpr.getDataType().isCoercibleTo(PDataType.LONG)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT)
            .setSchemaName(sequence.getSequenceName().getSchemaName())
            .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
        }
        final long incrementBy = (Long)PDataType.LONG.toObject(ptr, incrementByExpr.getDataType());
        
        long cacheSizeValue = connection.getQueryServices().getProps().getLong(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_SEQUENCE_CACHE_SIZE);
        if (cacheNode != null) {
            Expression cacheSizeExpr = cacheNode.accept(expressionCompiler);
            cacheSizeExpr.evaluate(null, ptr);
            if (ptr.getLength() != 0 && (!cacheSizeExpr.getDataType().isCoercibleTo(PDataType.LONG) || (cacheSizeValue = (Long)PDataType.LONG.toObject(ptr, cacheSizeExpr.getDataType())) < 0)) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT)
                .setSchemaName(sequence.getSequenceName().getSchemaName())
                .setTableName(sequence.getSequenceName().getTableName()).build().buildException();
            }
        }
        final long cacheSize = Math.max(1L, cacheSizeValue);
        

        final MetaDataClient client = new MetaDataClient(connection);        
        return new MutationPlan() {           

            @Override
            public MutationState execute() throws SQLException {
                return client.createSequence(sequence, startsWith, incrementBy, cacheSize);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return new ExplainPlan(Collections.singletonList("CREATE SEQUENCE"));
            }

            @Override
            public PhoenixConnection getConnection() {
                return connection;
            }

            @Override
            public ParameterMetaData getParameterMetaData() {                
                return context.getBindManager().getParameterMetaData();
            }

            @Override
            public StatementContext getContext() {
                return context;
            }
        };
    }
}