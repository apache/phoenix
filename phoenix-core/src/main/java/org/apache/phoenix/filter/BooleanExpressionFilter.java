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
package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ServerUtil;


/**
 * 
 * Base class for filter that evaluates a WHERE clause expression.
 *
 * Subclass is expected to implement filterRow() method
 * 
 * @since 0.1
 */
abstract public class BooleanExpressionFilter extends FilterBase implements Writable {

    protected Expression expression;
    private ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    
    public BooleanExpressionFilter() {
    }

    public BooleanExpressionFilter(Expression expression) {
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }
    
    @Override
    public boolean hasFilterRow() {
      return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + expression.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BooleanExpressionFilter other = (BooleanExpressionFilter)obj;
        if (!expression.equals(other.expression)) return false;
        return true;
    }

    @Override
    public String toString() {
        return expression.toString();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="NP_BOOLEAN_RETURN_NULL",
            justification="Returns null by design.")
    protected Boolean evaluate(Tuple input) {
        try {
            if (!expression.evaluate(input, tempPtr)) {
                return null;
            }
        } catch (IllegalDataException e) {
            return Boolean.FALSE;
        }
        return (Boolean)expression.getDataType().toObject(tempPtr);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        try {
            expression = ExpressionType.values()[WritableUtils.readVInt(input)].newInstance();
            expression.readFields(input);
        } catch (Throwable t) { // Catches incompatibilities during reading/writing and doesn't retry
            ServerUtil.throwIOException("BooleanExpressionFilter failed during reading", t);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        try {
            WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
            expression.write(output);
        } catch (Throwable t) { // Catches incompatibilities during reading/writing and doesn't retry
            ServerUtil.throwIOException("BooleanExpressionFilter failed during writing", t);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }

    @Override
    public void reset() {
        expression.reset();
    }
}
