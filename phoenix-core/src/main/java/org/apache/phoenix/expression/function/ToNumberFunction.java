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

import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;

import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.*;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;
import org.apache.phoenix.expression.Expression;
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
 * Implementation of TO_NUMBER(&lt;string&gt;/&lt;date&gt;/&lt;timestamp&gt;, [&lt;pattern-string&gt;]) built-in function.  The format for the optional
 * <code>pattern_string</code> param is specified in {@link DecimalFormat}.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=ToNumberFunction.NAME,  nodeClass=ToNumberParseNode.class, args= {
        @Argument(allowedTypes={PVarchar.class, PTimestamp.class}),
        @Argument(allowedTypes={PVarchar.class}, isConstant=true, defaultValue="null")} )
public class ToNumberFunction extends ScalarFunction {
	public static final String NAME = "TO_NUMBER";
    
    private String formatString = null;
    private Format format = null;
	private FunctionArgumentType type;
    
    public ToNumberFunction() {}

    public ToNumberFunction(List<Expression> children, FunctionArgumentType type, String formatString, Format formatter) throws SQLException {
        super(children.subList(0, 1));
        Preconditions.checkNotNull(type);
        this.type = type;
        this.formatString = formatString;
        this.format = formatter;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) {
            return true;
        }

        PDataType type = expression.getDataType();
        if (type.isCoercibleTo(PTimestamp.INSTANCE)) {
        	Date date = (Date) type.toObject(ptr, expression.getSortOrder());
        	BigDecimal time = new BigDecimal(date.getTime());
            byte[] byteValue = getDataType().toBytes(time);
            ptr.set(byteValue);
            return true;
        }
        
        String stringValue = (String)type.toObject(ptr, expression.getSortOrder());
        if (stringValue == null) {
            ptr.set(EMPTY_BYTE_ARRAY);
            return true;
        }
        stringValue = stringValue.trim();
        BigDecimal decimalValue;
        if (format == null) {
            decimalValue = (BigDecimal) getDataType().toObject(stringValue);
        } else {
            ParsePosition parsePosition = new ParsePosition(0);
            Number number = ((DecimalFormat) format).parse(stringValue, parsePosition);
            if (parsePosition.getErrorIndex() > -1) {
                ptr.set(EMPTY_BYTE_ARRAY);
                return true;
            }
            
            if (number instanceof BigDecimal) { 
                // since we set DecimalFormat.setParseBigDecimal(true) we are guaranteeing result to be 
                // of type BigDecimal in most cases.  see java.text.DecimalFormat.parse() JavaDoc.
                decimalValue = (BigDecimal)number;
            } else {
                ptr.set(EMPTY_BYTE_ARRAY);
                return true;
            }
        }
        byte[] byteValue = getDataType().toBytes(decimalValue);
        ptr.set(byteValue);
        return true;
    }

    @Override
    public PDataType getDataType() {
    	return PDecimal.INSTANCE;
    }
    
    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        formatString = WritableUtils.readString(input);
        type = WritableUtils.readEnum(input, FunctionArgumentType.class);
        if (formatString != null) {
        	format = type.getFormatter(formatString);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, formatString);
        WritableUtils.writeEnum(output, type);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((formatString == null) ? 0 : formatString.hashCode());
        result = prime * result + getExpression().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        ToNumberFunction other = (ToNumberFunction)obj;
        if (formatString == null) {
            if (other.formatString != null) return false;
        } else if (!formatString.equals(other.formatString)) return false;
        if (!getExpression().equals(other.getExpression())) return false;
        return true;
    }
}
