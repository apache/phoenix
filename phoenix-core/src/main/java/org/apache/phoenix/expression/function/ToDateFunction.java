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
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.DateUtil;


/**
 * 
 * Implementation of the TO_DATE(<string>,[<format-string>]) built-in function.
 * The second argument is optional and defaults to the phoenix.query.dateFormat value
 * from the HBase config. If present it must be a constant string.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=ToDateFunction.NAME, nodeClass=ToDateParseNode.class, args= {@Argument(allowedTypes={PDataType.VARCHAR}),@Argument(allowedTypes={PDataType.VARCHAR},isConstant=true,defaultValue="null")} )
public class ToDateFunction extends ScalarFunction {
    public static final String NAME = "TO_DATE";
    private Format dateParser;
    private String dateFormat;

    public ToDateFunction() {
    }

    public ToDateFunction(List<Expression> children, String dateFormat, Format dateParser) throws SQLException {
        super(children.subList(0, 1));
        this.dateFormat = dateFormat;
        this.dateParser = dateParser;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dateFormat.hashCode();
        result = prime * result + getExpression().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ToDateFunction other = (ToDateFunction)obj;
        if (!getExpression().equals(other.getExpression())) return false;
        if (!dateFormat.equals(other.dateFormat)) return false;
        return true;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression expression = getExpression();
        if (!expression.evaluate(tuple, ptr) || ptr.getLength() == 0) {
            return false;
        }
        PDataType type = expression.getDataType();
        String dateStr = (String)type.toObject(ptr, expression.getSortOrder());
        try {
            Object value = dateParser.parseObject(dateStr);
            byte[] byteValue = getDataType().toBytes(value);
            ptr.set(byteValue);
            return true;
        } catch (ParseException e) {
            throw new IllegalStateException("to_date('" + dateStr + ")' did not match expected date format of '" + dateFormat + "'.");
        }
     }

    @Override
    public PDataType getDataType() {
        return PDataType.DATE;
    }

    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        dateFormat = WritableUtils.readString(input);
        dateParser = DateUtil.getDateParser(dateFormat);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, dateFormat);
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
