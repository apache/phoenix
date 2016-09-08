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
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

/**
 * 
 * Base class for functions that use joda time. 
 * Used primarily by FLOOR , ROUND and CEIL on the time units WEEK,MONTH and YEAR. 
 */
public abstract class RoundJodaDateExpression extends RoundDateExpression{

    public RoundJodaDateExpression(){}
    
    public RoundJodaDateExpression(List<Expression> children) {
       super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (children.get(0).evaluate(tuple, ptr)) {
            if (ptr.getLength() == 0) {
                return true; // child evaluated to null
            }
            PDataType dataType = getDataType();
            long time = dataType.getCodec().decodeLong(ptr, children.get(0).getSortOrder());
            DateTime dt = new DateTime(time,ISOChronology.getInstanceUTC());
            long value = roundDateTime(dt);
            Date d = new Date(value);
            byte[] byteValue = dataType.toBytes(d);
            ptr.set(byteValue);
            return true;
        }
        return false;
    }
    
    /**
     * @param dateTime
     * @return Time in millis.
     */
    public abstract long roundDateTime(DateTime dateTime);
}
