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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.ToDateParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.DateUtil;


/**
 *
 * Implementation of the {@code TO_DATE(<string>,[<format-string>,[<timezone-string>]])} built-in function.
 * The second argument is optional and defaults to the phoenix.query.dateFormat value
 * from the HBase config. If present it must be a constant string. The third argument is either a
 * valid (constant) timezone id, or the string "local". The third argument is also optional, and
 * it defaults to GMT.
 *
 */
@BuiltInFunction(name=ToDateFunction.NAME, nodeClass=ToDateParseNode.class,
        args={@Argument(allowedTypes={PVarchar.class}),
                @Argument(allowedTypes={PVarchar.class},isConstant=true,defaultValue="null"),
                @Argument(allowedTypes={PVarchar.class}, isConstant=true, defaultValue = "null") } )
public class ToDateFunction extends ScalarFunction {
    public static final String NAME = "TO_DATE";
    private DateUtil.DateTimeParser dateParser;
    private String dateFormat;
    private String timeZoneId;

    public ToDateFunction() {
    }

    public ToDateFunction(List<Expression> children, String dateFormat, String timeZoneId) throws SQLException {
        super(children);
        init(dateFormat, timeZoneId);
    }
    
    private void init(String dateFormat, String timeZoneId) {
        this.dateFormat = dateFormat;
        this.dateParser = DateUtil.getDateTimeParser(dateFormat, getDataType(), timeZoneId);
        // Store resolved timeZoneId, as if it's LOCAL, we don't want the
        // server to evaluate using the local time zone. Instead, we want
        // to use the client local time zone.
        this.timeZoneId = this.dateParser.getTimeZone().getID();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((dateFormat == null) ? 0 : dateFormat.hashCode());
        result = prime * result + ((timeZoneId == null) ? 0 : timeZoneId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (getClass() != obj.getClass()) return false;
        ToDateFunction other = (ToDateFunction)obj;
        // Only compare first child, as the other two are potentially resolved on the fly.
        if (!this.getChildren().get(0).equals(other.getChildren().get(0))) return false;
        if (dateFormat == null) {
            if (other.dateFormat != null) return false;
        } else if (!dateFormat.equals(other.dateFormat)) return false;
        if (timeZoneId == null) {
            if (other.timeZoneId != null) return false;
        } else if (!timeZoneId.equals(other.timeZoneId)) return false;
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
        long epochTime = dateParser.parseDateTime(dateStr);
        PDataType returnType = getDataType();
        byte[] byteValue = new byte[returnType.getByteSize()];
        returnType.getCodec().encodeLong(epochTime, byteValue, 0);
        ptr.set(byteValue);
        return true;
     }

    @Override
    public PDataType getDataType() {
        return PDate.INSTANCE;
    }

    @Override
    public boolean isNullable() {
        return getExpression().isNullable();
    }

    private String getTimeZoneIdArg() {
        return children.size() < 3 ? null : (String) ((LiteralExpression) children.get(2)).getValue();
    }
    
    private String getDateFormatArg() {
        return children.size() < 2 ? null : (String) ((LiteralExpression) children.get(1)).getValue();
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        String timeZoneId;
        String dateFormat = WritableUtils.readString(input);  
        if (dateFormat.length() != 0) { // pre 4.3
            timeZoneId = DateUtil.DEFAULT_TIME_ZONE_ID;         
        } else {
            int nChildren = children.size();
            if (nChildren == 1) {
                dateFormat = WritableUtils.readString(input); 
                timeZoneId =  WritableUtils.readString(input);
            } else if (nChildren == 2 || DateUtil.LOCAL_TIME_ZONE_ID.equalsIgnoreCase(getTimeZoneIdArg())) {
                dateFormat = getDateFormatArg();
                timeZoneId =  WritableUtils.readString(input);
            } else {
                dateFormat = getDateFormatArg();
                timeZoneId =  getTimeZoneIdArg();
            }
        }
        init(dateFormat, timeZoneId);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, ""); // For b/w compat
        int nChildren = children.size();
        // If dateFormat and/or timeZoneId are supplied as children, don't write them again,
        // except if using LOCAL, in which case we want to write the resolved/actual time zone.
        if (nChildren == 1) {
            WritableUtils.writeString(output, dateFormat);
            WritableUtils.writeString(output, timeZoneId);
        } else if (nChildren == 2 || DateUtil.LOCAL_TIME_ZONE_ID.equalsIgnoreCase(getTimeZoneIdArg())) {
            WritableUtils.writeString(output, timeZoneId);
        }
    }

    private Expression getExpression() {
        return children.get(0);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
