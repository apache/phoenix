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

import java.sql.Date;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * Returns offset (shift in minutes) of timezone at particular datetime in minutes.
 */
@FunctionParseNode.BuiltInFunction(name = TimezoneOffsetFunction.NAME, args = {
    @FunctionParseNode.Argument(allowedTypes = {PDataType.VARCHAR}),
    @FunctionParseNode.Argument(allowedTypes = {PDataType.DATE})})
public class TimezoneOffsetFunction extends ScalarFunction {

    public static final String NAME = "TIMEZONE_OFFSET";
    private static final int MILLIS_TO_MINUTES = 60 * 1000;
    private final Map<String, TimeZone> cachedTimeZones = new HashMap<String, TimeZone>();

    public TimezoneOffsetFunction() {
    }

    public TimezoneOffsetFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!children.get(0).evaluate(tuple, ptr)) {
            return false;
        }

        String timezone = Bytes.toString(ptr.get(), ptr.getOffset(), ptr.getLength());

        if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }

        if (!cachedTimeZones.containsKey(timezone)) {
            TimeZone tz = TimeZone.getTimeZone(timezone);
            if (!tz.getID().equals(timezone)) {
                throw new IllegalDataException("Invalid timezone " + timezone);
            }
            cachedTimeZones.put(timezone, tz);
        }

		Date date = (Date)PDataType.DATE.toObject(ptr, children.get(1).getSortOrder());
		int offset = cachedTimeZones.get(timezone).getOffset(date.getTime());

        ptr.set(PDataType.INTEGER.toBytes(offset / MILLIS_TO_MINUTES));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.INTEGER;
    }

	@Override
    public boolean isNullable() {
        return children.get(0).isNullable() || children.get(1).isNullable();
    }

}
