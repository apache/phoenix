/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;
import org.joda.time.DateTimeZone;
import org.joda.time.tz.CachedDateTimeZone;

/**
 * Build in function CONVERT_TZ(date, 'timezone_from', 'timezone_to). Convert date from one timezone to
 * another
 *
 */
@FunctionParseNode.BuiltInFunction(name = ConvertTimezoneFunction.NAME, args = {
    @FunctionParseNode.Argument(allowedTypes = { PDate.class }),
    @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
    @FunctionParseNode.Argument(allowedTypes = { PVarchar.class })})
public class ConvertTimezoneFunction extends ScalarFunction {

    public static final String NAME = "CONVERT_TZ";
    private static final Map<byte[], DateTimeZone> cachedJodaTimeZones = new HashMap<>();

    public ConvertTimezoneFunction() {
    }

    public ConvertTimezoneFunction(List<Expression> children) throws SQLException {
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
        long date = PDate.INSTANCE.getCodec().decodeLong(ptr, children.get(0).getSortOrder());

        if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
        DateTimeZone timezoneFrom = getJodaTimezoneFromCache(ptr.copyBytes());

        if (!children.get(2).evaluate(tuple, ptr)) {
            return false;
        }
        DateTimeZone timezoneTo = getJodaTimezoneFromCache(ptr.copyBytes());

        long convertedDate = date - timezoneFrom.getOffset(date) + timezoneTo.getOffset(date);
        PDate.INSTANCE.getCodec().encodeLong(convertedDate, ptr);
        return true;
    }

    private static DateTimeZone getJodaTimezoneFromCache(byte[] timezone) {
        DateTimeZone jodaTimezone = cachedJodaTimeZones.get(timezone);
        if (jodaTimezone == null) {
            try {
                //cache timezone instance
                DateTimeZone tz = CachedDateTimeZone.forID(Bytes.toString(timezone));
                cachedJodaTimeZones.put(timezone, tz);
                return tz;
            } catch (IllegalArgumentException e) {
                throw new IllegalDataException("Unknown timezone " + Bytes.toString(timezone), e);
            }
        }
        return jodaTimezone;
    }

    @Override
    public PDataType getDataType() {
        return PDate.INSTANCE;
    }
}
