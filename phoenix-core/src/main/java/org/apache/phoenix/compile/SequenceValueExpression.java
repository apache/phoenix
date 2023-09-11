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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.parse.SequenceValueParseNode.Op;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.SchemaUtil;

public class SequenceValueExpression extends BaseTerminalExpression {
    private final SequenceKey key;
    final Op op;
    private final int index;
    private final Set<Expression> numToAllocateExpressions = new HashSet<>();

    public SequenceValueExpression(SequenceKey key, Op op, int index, Expression numToAllocateExp) {
        this.key = key;
        this.op = op;
        this.index = index;
        this.numToAllocateExpressions.add(numToAllocateExp);
    }

    public SequenceValueExpression(SequenceValueExpression seqIn, Op op, Expression numToAllocateExp) {
        this.key = seqIn.getKey();
        this.op = op;
        this.index = seqIn.getIndex();
        this.numToAllocateExpressions.addAll(seqIn.numToAllocateExpressions);
        this.numToAllocateExpressions.add(numToAllocateExp);
    }

    public Set<Expression> getNumToAllocateExpressions() {
        return numToAllocateExpressions;
    }
    
    public SequenceKey getKey() {
        return key;
    }
    
    public int getIndex() {
        return index;
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    		byte[] valueBuffer = new byte[PLong.INSTANCE.getByteSize()];
        PLong.INSTANCE.getCodec().encodeLong(tuple.getSequenceValue(index), valueBuffer, 0);
        ptr.set(valueBuffer);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
    }
    
    @Override
    public boolean isNullable() {
        return false;
    }
    
    @Override
    public Determinism getDeterminism() {
        return Determinism.PER_ROW;
    }
    
    @Override
    public boolean isStateless() {
        return true;
    }

    @Override
    public String toString() {
        String sequenceQualifiedName =
                SchemaUtil.getTableName(key.getSchemaName(), key.getSequenceName());
        if (op == Op.CURRENT_VALUE) {
            return op.getName() + " VALUE " + "FOR " + sequenceQualifiedName;
        } else {
            return op.getName() + Arrays.toString(getNumToAllocateExpressions().toArray())
                    + " VALUE(S) " + "FOR " + sequenceQualifiedName;
        }
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}