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
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.SortOrder;

import org.apache.phoenix.util.SequenceUtil;

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
            return PLong.INSTANCE;
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
            return PInteger.INSTANCE;
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

    private void validateNodeIsStateless(CreateSequenceStatement sequence, ParseNode node,
            SQLExceptionCode code) throws SQLException {
        if (!node.isStateless()) {
            TableName sequenceName = sequence.getSequenceName();
            throw SequenceUtil.getException(sequenceName.getSchemaName(), sequenceName.getTableName(), code);
        }
    }

    private long evalExpression(CreateSequenceStatement sequence, StatementContext context,
            Expression expression, SQLExceptionCode code) throws SQLException {
        ImmutableBytesWritable ptr = context.getTempPtr();
        expression.evaluate(null, ptr);
        if (ptr.getLength() == 0 || !expression.getDataType().isCoercibleTo(PLong.INSTANCE)) {
            TableName sequenceName = sequence.getSequenceName();
            throw SequenceUtil.getException(sequenceName.getSchemaName(), sequenceName.getTableName(), code);
        }
        return (Long) PLong.INSTANCE.toObject(ptr, expression.getDataType());
    }

    public MutationPlan compile(final CreateSequenceStatement sequence) throws SQLException {
        ParseNode startsWithNode = sequence.getStartWith();
        ParseNode incrementByNode = sequence.getIncrementBy();
        ParseNode maxValueNode = sequence.getMaxValue();
        ParseNode minValueNode = sequence.getMinValue();
        ParseNode cacheNode = sequence.getCacheSize();

        // validate parse nodes
        if (startsWithNode!=null) {
            validateNodeIsStateless(sequence, startsWithNode,
                SQLExceptionCode.START_WITH_MUST_BE_CONSTANT);
        }
        validateNodeIsStateless(sequence, incrementByNode,
            SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT);
        validateNodeIsStateless(sequence, maxValueNode, 
            SQLExceptionCode.MAXVALUE_MUST_BE_CONSTANT);
        validateNodeIsStateless(sequence, minValueNode, 
            SQLExceptionCode.MINVALUE_MUST_BE_CONSTANT);
        if (cacheNode != null) {
            validateNodeIsStateless(sequence, cacheNode,
                SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT);
        }

        final PhoenixConnection connection = statement.getConnection();
        final StatementContext context = new StatementContext(statement);
        
        // add param meta data if required
        if (startsWithNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode) startsWithNode, LONG_DATUM);
        }
        if (incrementByNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode) incrementByNode, LONG_DATUM);
        }
        if (maxValueNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode) maxValueNode, LONG_DATUM);
        }
        if (minValueNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode) minValueNode, LONG_DATUM);
        }
        if (cacheNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode) cacheNode, INTEGER_DATUM);
        }
        
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);        
        final long incrementBy =
                evalExpression(sequence, context, incrementByNode.accept(expressionCompiler),
                    SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT);
        if (incrementBy == 0) {
            throw SequenceUtil.getException(sequence.getSequenceName().getSchemaName(), sequence
                    .getSequenceName().getTableName(),
                SQLExceptionCode.INCREMENT_BY_MUST_NOT_BE_ZERO);
        }
        final long maxValue =
                evalExpression(sequence, context, maxValueNode.accept(expressionCompiler),
                    SQLExceptionCode.MAXVALUE_MUST_BE_CONSTANT);
        final long minValue =
                evalExpression(sequence, context, minValueNode.accept(expressionCompiler),
                    SQLExceptionCode.MINVALUE_MUST_BE_CONSTANT);
        if (minValue>maxValue) {
            TableName sequenceName = sequence.getSequenceName();
            throw SequenceUtil.getException(sequenceName.getSchemaName(),
                sequenceName.getTableName(),
                SQLExceptionCode.MINVALUE_MUST_BE_LESS_THAN_OR_EQUAL_TO_MAXVALUE);
        }
        
        long startsWithValue;
        if (startsWithNode == null) {
            startsWithValue = incrementBy > 0 ? minValue : maxValue;
        } else {
            startsWithValue =
                    evalExpression(sequence, context, startsWithNode.accept(expressionCompiler),
                        SQLExceptionCode.START_WITH_MUST_BE_CONSTANT);
            if (startsWithValue < minValue || startsWithValue > maxValue) {
                TableName sequenceName = sequence.getSequenceName();
                throw SequenceUtil.getException(sequenceName.getSchemaName(),
                    sequenceName.getTableName(),
                    SQLExceptionCode.STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE);
            }
        }
        final long startsWith = startsWithValue;

        long cacheSizeValue;
        if (cacheNode == null) {
            cacheSizeValue =
                    connection
                            .getQueryServices()
                            .getProps()
                            .getLong(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB,
                                QueryServicesOptions.DEFAULT_SEQUENCE_CACHE_SIZE);
        }
        else {
            cacheSizeValue = 
                    evalExpression(sequence, context, cacheNode.accept(expressionCompiler),
                        SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT);
            if (cacheSizeValue < 0) {
                TableName sequenceName = sequence.getSequenceName();
                throw SequenceUtil.getException(sequenceName.getSchemaName(),
                    sequenceName.getTableName(),
                    SQLExceptionCode.CACHE_MUST_BE_NON_NEGATIVE_CONSTANT);
            }
        }
        final long cacheSize = Math.max(1L, cacheSizeValue);

        final MetaDataClient client = new MetaDataClient(connection);
        return new MutationPlan() {

            @Override
            public MutationState execute() throws SQLException {
                return client.createSequence(sequence, startsWith, incrementBy, cacheSize, minValue, maxValue);
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