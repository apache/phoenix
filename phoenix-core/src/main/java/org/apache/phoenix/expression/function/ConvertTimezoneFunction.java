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
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.cache.JodaTimezoneCache;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;
import org.joda.time.DateTimeZone;

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
        DateTimeZone timezoneFrom = JodaTimezoneCache.getInstance(ptr);

        if (!children.get(2).evaluate(tuple, ptr)) {
            return false;
        }
        DateTimeZone timezoneTo = JodaTimezoneCache.getInstance(ptr);

        long convertedDate = date - timezoneFrom.getOffset(date) + timezoneTo.getOffset(date);
        byte[] outBytes = new byte[8];
        PDate.INSTANCE.getCodec().encodeLong(convertedDate, outBytes, 0);
        ptr.set(outBytes);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDate.INSTANCE;
    }
}
