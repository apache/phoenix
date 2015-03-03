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
package org.apache.phoenix.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

public class HashJoinInfo {
    private static final String HASH_JOIN = "HashJoin";

    private KeyValueSchema joinedSchema;
    private ImmutableBytesPtr[] joinIds;
    private List<Expression>[] joinExpressions;
    private JoinType[] joinTypes;
    private boolean[] earlyEvaluation;
    private KeyValueSchema[] schemas;
    private int[] fieldPositions;
    private Expression postJoinFilterExpression;
    private Integer limit;

    public HashJoinInfo(PTable joinedTable, ImmutableBytesPtr[] joinIds, List<Expression>[] joinExpressions, JoinType[] joinTypes, boolean[] earlyEvaluation, PTable[] tables, int[] fieldPositions, Expression postJoinFilterExpression, Integer limit) {
    	this(buildSchema(joinedTable), joinIds, joinExpressions, joinTypes, earlyEvaluation, buildSchemas(tables), fieldPositions, postJoinFilterExpression, limit);
    }

    private static KeyValueSchema[] buildSchemas(PTable[] tables) {
    	KeyValueSchema[] schemas = new KeyValueSchema[tables.length];
    	for (int i = 0; i < tables.length; i++) {
    		schemas[i] = buildSchema(tables[i]);
    	}
    	return schemas;
    }

    private static KeyValueSchema buildSchema(PTable table) {
    	KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
    	if (table != null) {
    	    for (PColumn column : table.getColumns()) {
    	        if (!SchemaUtil.isPKColumn(column)) {
    	            builder.addField(column);
    	        }
    	    }
    	}
        return builder.build();
    }

    private HashJoinInfo(KeyValueSchema joinedSchema, ImmutableBytesPtr[] joinIds, List<Expression>[] joinExpressions, JoinType[] joinTypes, boolean[] earlyEvaluation, KeyValueSchema[] schemas, int[] fieldPositions, Expression postJoinFilterExpression, Integer limit) {
    	this.joinedSchema = joinedSchema;
    	this.joinIds = joinIds;
        this.joinExpressions = joinExpressions;
        this.joinTypes = joinTypes;
        this.earlyEvaluation = earlyEvaluation;
        this.schemas = schemas;
        this.fieldPositions = fieldPositions;
        this.postJoinFilterExpression = postJoinFilterExpression;
        this.limit = limit;
    }

    public KeyValueSchema getJoinedSchema() {
    	return joinedSchema;
    }

    public ImmutableBytesPtr[] getJoinIds() {
        return joinIds;
    }

    public List<Expression>[] getJoinExpressions() {
        return joinExpressions;
    }

    public JoinType[] getJoinTypes() {
        return joinTypes;
    }

    public boolean[] earlyEvaluation() {
    	return earlyEvaluation;
    }

    public KeyValueSchema[] getSchemas() {
    	return schemas;
    }

    public int[] getFieldPositions() {
    	return fieldPositions;
    }

    public Expression getPostJoinFilterExpression() {
        return postJoinFilterExpression;
    }

    public Integer getLimit() {
        return limit;
    }
    
    public boolean forceProjection() {
        return true;
    }
 
    public static void serializeHashJoinIntoScan(Scan scan, HashJoinInfo joinInfo) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            joinInfo.joinedSchema.write(output);
            int count = joinInfo.joinIds.length;
            WritableUtils.writeVInt(output, count);
            for (int i = 0; i < count; i++) {
                joinInfo.joinIds[i].write(output);
                WritableUtils.writeVInt(output, joinInfo.joinExpressions[i].size());
                for (Expression expr : joinInfo.joinExpressions[i]) {
                    WritableUtils.writeVInt(output, ExpressionType.valueOf(expr).ordinal());
                    expr.write(output);
                }
                WritableUtils.writeVInt(output, joinInfo.joinTypes[i].ordinal());
                output.writeBoolean(joinInfo.earlyEvaluation[i]);
                joinInfo.schemas[i].write(output);
                WritableUtils.writeVInt(output, joinInfo.fieldPositions[i]);
            }
            if (joinInfo.postJoinFilterExpression != null) {
                WritableUtils.writeVInt(output, ExpressionType.valueOf(joinInfo.postJoinFilterExpression).ordinal());
                joinInfo.postJoinFilterExpression.write(output);
            } else {
                WritableUtils.writeVInt(output, -1);
            }
            WritableUtils.writeVInt(output, joinInfo.limit == null ? -1 : joinInfo.limit);
            output.writeBoolean(true);
            scan.setAttribute(HASH_JOIN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @SuppressWarnings("unchecked")
    public static HashJoinInfo deserializeHashJoinFromScan(Scan scan) {
        byte[] join = scan.getAttribute(HASH_JOIN);
        if (join == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(join);
        try {
            DataInputStream input = new DataInputStream(stream);
            KeyValueSchema joinedSchema = new KeyValueSchema();
            joinedSchema.readFields(input);
            int count = WritableUtils.readVInt(input);
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            boolean[] earlyEvaluation = new boolean[count];
            KeyValueSchema[] schemas = new KeyValueSchema[count];
            int[] fieldPositions = new int[count];
            for (int i = 0; i < count; i++) {
                joinIds[i] = new ImmutableBytesPtr();
                joinIds[i].readFields(input);
                int nExprs = WritableUtils.readVInt(input);
                joinExpressions[i] = new ArrayList<Expression>(nExprs);
                for (int j = 0; j < nExprs; j++) {
                    int expressionOrdinal = WritableUtils.readVInt(input);
                    Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(input);
                    joinExpressions[i].add(expression);
                }
                int type = WritableUtils.readVInt(input);
                joinTypes[i] = JoinType.values()[type];
                earlyEvaluation[i] = input.readBoolean();
                schemas[i] = new KeyValueSchema();
                schemas[i].readFields(input);
                fieldPositions[i] = WritableUtils.readVInt(input);
            }
            Expression postJoinFilterExpression = null;
            int expressionOrdinal = WritableUtils.readVInt(input);
            if (expressionOrdinal != -1) {
                postJoinFilterExpression = ExpressionType.values()[expressionOrdinal].newInstance();
                postJoinFilterExpression.readFields(input);
            }
            int limit = -1;
            // Read these and ignore if we don't find them as they were not
            // present in Apache Phoenix 3.0.0 release. This allows a newer
            // 3.1 server to work with an older 3.0 client without force
            // both to be upgraded in lock step.
            try {
                limit = WritableUtils.readVInt(input);
                input.readBoolean(); // discarded info in new versions
            } catch (EOFException ignore) {
            }
            return new HashJoinInfo(joinedSchema, joinIds, joinExpressions, joinTypes, earlyEvaluation, schemas, fieldPositions, postJoinFilterExpression, limit >= 0 ? limit : null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
