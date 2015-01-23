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
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.util.ExpressionUtil;


/**
 * 
 * CASE/WHEN expression implementation
 *
 * 
 * @since 0.1
 */
public class CaseExpression extends BaseCompoundExpression {
    private static final int FULLY_EVALUATE = -1;
    
    private short evalIndex = FULLY_EVALUATE;
    private boolean foundIndex;
    private PDataType returnType;
   
    public CaseExpression() {
    }
    
    public static Expression create(List<Expression> children) throws SQLException {
        CaseExpression caseExpression = new CaseExpression(coerceIfNecessary(children));
        if (ExpressionUtil.isConstant(caseExpression)) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            int index = caseExpression.evaluateIndexOf(null, ptr);
            if (index < 0) {
                return LiteralExpression.newConstant(null, caseExpression.getDeterminism());
            }
            return caseExpression.getChildren().get(index);
        }
        return caseExpression;
    }
    
    private static List<Expression> coerceIfNecessary(List<Expression> children) throws SQLException {
        boolean isChildTypeUnknown = false;
        PDataType returnType = children.get(0).getDataType();
        for (int i = 2; i < children.size(); i+=2) {
            Expression child = children.get(i);
            PDataType childType = child.getDataType();
            if (childType == null) {
                isChildTypeUnknown = true;
            } else if (returnType == null) {
                returnType = childType;
                isChildTypeUnknown = true;
            } else if (returnType == childType || childType.isCoercibleTo(returnType)) {
                continue;
            } else if (returnType.isCoercibleTo(childType)) {
                returnType = childType;
            } else {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
                    .setMessage("Case expressions must have common type: " + returnType + " cannot be coerced to " + childType)
                    .build().buildException();
            }
        }
        // If we found an "unknown" child type and the return type is a number
        // make the return type be the most general number type of DECIMAL.
        if (isChildTypeUnknown && returnType != null && returnType.isCoercibleTo(PDecimal.INSTANCE)) {
            returnType = PDecimal.INSTANCE;
        }
        List<Expression> newChildren = children;
        for (int i = 0; i < children.size(); i+=2) {
            Expression child = children.get(i);
            PDataType childType = child.getDataType();
            if (childType != returnType) {
                if (newChildren == children) {
                    newChildren = new ArrayList<Expression>(children);
                }
                newChildren.set(i, CoerceExpression.create(child, returnType));
            }
        }
        return newChildren;
    }
    /**
     * Construct CASE/WHEN expression
     * @param expressions list of expressions in the form of:
     *  ((<result expression>, <boolean expression>)+, [<optional else result expression>])
     * @throws SQLException if return type of case expressions do not match and cannot
     *  be coerced to a common type
     */
    public CaseExpression(List<Expression> children) {
        super(children);
        returnType = children.get(0).getDataType();
    }
    
    private boolean isPartiallyEvaluating() {
        return evalIndex != FULLY_EVALUATE;
    }
    
    public boolean hasElse() {
        return children.size() % 2 != 0;
    }
    
    @Override
    public boolean isNullable() {
        // If any expression is nullable or there's no else clause
        // return true since null may be returned.
        if (super.isNullable() || !hasElse()) {
            return true;
        }
        return children.get(children.size()-1).isNullable();
    }

    @Override
    public PDataType getDataType() {
        return returnType;
    }

    @Override
    public void reset() {
        foundIndex = false;
        evalIndex = 0;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.returnType = PDataType.values()[WritableUtils.readVInt(input)];
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, this.returnType.ordinal());
    }
    
    public int evaluateIndexOf(Tuple tuple, ImmutableBytesWritable ptr) {
        if (foundIndex) {
            return evalIndex;
        }
        int size = children.size();
        // If we're doing partial evaluation, start where we left off
        for (int i = isPartiallyEvaluating() ? evalIndex : 0; i < size; i+=2) {
            // Short circuit if we see our stop value
            if (i+1 == size) {
                return i;
            }
            // If we get null, we have to re-evaluate from that point (special case this in filter, like is null)
            // We may only run this when we're done/have all values
            boolean evaluated = children.get(i+1).evaluate(tuple, ptr);
            if (evaluated && Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr))) {
                if (isPartiallyEvaluating()) {
                    foundIndex = true;
                }
                return i;
            }
            if (isPartiallyEvaluating()) {
                if (evaluated || tuple.isImmutable()) {
                    evalIndex+=2;
                } else {
                    /*
                     * Return early here if incrementally evaluating and we don't
                     * have all the key values yet. We can't continue because we'd
                     * potentially be bypassing cases which we could later evaluate
                     * once we have more column values.
                     */
                    return -1;
                }
            }
        }
        // No conditions matched, return size to indicate that we were able
        // to evaluate all cases, but didn't find any matches.
        return size;
    }
    
    /**
     * Only expression that currently uses the isPartial flag. The IS NULL
     * expression will use it too. TODO: We could alternatively have a non interface
     * method, like setIsPartial in which we set to false prior to calling
     * evaluate.
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        int index = evaluateIndexOf(tuple, ptr);
        if (index < 0) {
            return false;
        } else if (index == children.size()) {
            ptr.set(PDataType.NULL_BYTES);
            return true;
        }
        if (children.get(index).evaluate(tuple, ptr)) {
            return true;
        }
        return false;
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("CASE ");
        for (int i = 0; i < children.size() - 1; i+=2) {
            buf.append("WHEN ");
            buf.append(children.get(i+1));
            buf.append(" THEN ");
            buf.append(children.get(i));
        }
        if (hasElse()) {
            buf.append(" ELSE " + children.get(children.size()-1));
        }
        buf.append(" END");
        return buf.toString();
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return super.requiresFinalEvaluation() || this.hasElse();
    }
}
