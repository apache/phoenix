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
package org.apache.phoenix.expression.function;

import java.io.*;
import java.sql.SQLException;
import java.text.Format;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Implementation of the TO_CHAR(&lt;date&gt;/&lt;number&gt;,[&lt;format-string&gt;] built-in function.
 * The first argument must be of type DATE or TIME or TIMESTAMP or DECIMAL or INTEGER, and the second argument must be a constant string. 
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=ToCharFunction.NAME, nodeClass=ToCharParseNode.class, args={
    @Argument(allowedTypes={PTimestamp.class, PDecimal.class}),
    @Argument(allowedTypes={PVarchar.class},isConstant=true,defaultValue="null") } )
public class ToCharFunction extends ScalarFunction {
    public static final String NAME = "TO_CHAR";
    private String formatString;
    private Format formatter;
    private FunctionArgumentType type;
    
    public ToCharFunction() {
    }

    public ToCharFunction(List<Expression> children, StatementContext context) throws SQLException {
        super(children.subList(0, 1));
        PDataType dataType = children.get(0).getDataType();
        String formatString = (String)((LiteralExpression)children.get(1)).getValue(); // either date or number format string
        Format formatter;
        FunctionArgumentType type;
        if (dataType.isCoercibleTo(PTimestamp.INSTANCE)) {
            if (formatString == null) {
                formatString = context.getDateFormat();
                formatter = context.getDateFormatter();
            } else {
                formatter = FunctionArgumentType.TEMPORAL.getFormatter(formatString);
            }
            type = FunctionArgumentType.TEMPORAL;
        }
        else if (dataType.isCoercibleTo(PDecimal.INSTANCE)) {
            if (formatString == null)
                formatString = context.getNumberFormat();
            formatter = FunctionArgumentType.NUMERIC.getFormatter(formatString);
            type = FunctionArgumentType.NUMERIC;
        }
        else {
            throw new SQLException(dataType + " type is unsupported for TO_CHAR().  Numeric and temporal types are supported.");
        }
        Preconditions.checkNotNull(formatString);
        Preconditions.checkNotNull(formatter);
        Preconditions.checkNotNull(type);
        this.type = type;
        this.formatString = formatString;
        this.formatter = formatter;
    }

    public ToCharFunction(List<Expression> children, FunctionArgumentType type, String formatString, Format formatter) throws SQLException {
        super(children.subList(0, 1));
        Preconditions.checkNotNull(formatString);
        Preconditions.checkNotNull(formatter);
        Preconditions.checkNotNull(type);
        this.type = type;
        this.formatString = formatString;
        this.formatter = formatter;
    }
    
    @Override
    public ToCharFunction clone(List<Expression> children) {
    	try {
            return new ToCharFunction(children, type, formatString, formatter);
        } catch (Exception e) {
            throw new RuntimeException(e); // Impossible, since it was originally constructed this way
        }
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + formatString.hashCode();
        result = prime * result + getExpression().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ToCharFunction other = (ToCharFunction)obj;
        if (!getExpression().equals(other.getExpression())) return false;
        if (!formatString.equals(other.formatString)) return false;
        return true;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        PDataType type = expression.getDataType();
        Object value = formatter.format(type.toObject(ptr, expression.getSortOrder()));
        byte[] b = getDataType().toBytes(value);
        ptr.set(b);
        return true;
     }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        formatString = WritableUtils.readString(input);
        type = WritableUtils.readEnum(input, FunctionArgumentType.class);
        formatter = type.getFormatter(formatString);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, formatString);
        WritableUtils.writeEnum(output, type);
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
