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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A container for a column that appears in ORDER BY clause.
 */
public class OrderByExpression implements Writable {
    private Expression expression;
    private boolean isNullsLast;
    private boolean isAscending;
    
    public OrderByExpression() {
    }
    
    public OrderByExpression(Expression expression, boolean isNullsLast, boolean isAcending) {
        checkNotNull(expression);
        this.expression = expression;
        this.isNullsLast = isNullsLast;
        this.isAscending = isAcending;
    }

    public Expression getExpression() {
        return expression;
    }
    
    public boolean isNullsLast() {
        return isNullsLast;
    }
    
    public boolean isAscending() {
        return isAscending;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o != null && this.getClass() == o.getClass()) {
            OrderByExpression that = (OrderByExpression)o;
            return isNullsLast == that.isNullsLast
                && isAscending == that.isAscending
                && expression.equals(that.expression);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isNullsLast ? 0 : 1);
        result = prime * result + (isAscending ? 0 : 1);
        result = prime * result + expression.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        return this.getExpression() + (isAscending ? "" : " DESC") + (isNullsLast ? " NULLS LAST" : "");
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        this.isNullsLast = input.readBoolean();
        this.isAscending = input.readBoolean();
        expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
        expression.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeBoolean(isNullsLast);
        output.writeBoolean(isAscending);
        WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
        expression.write(output);
    }

}