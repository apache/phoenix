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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;

import java.sql.SQLException;
import java.util.List;


/**
 * 
 * Function used to get the external SQL type id from the internal SQL type integer.
 * Typically the external and internal ids are the same, but for some types (e.g. arrays)
 * there is are multiple specific internal types to represent multiple external types.
 * <p/>
 * Usage:
 * ExternalSqlTypeId(12)
 * will return 12 based on {@link java.sql.Types#VARCHAR} being 12
 * 
 * 
 * @since 3.0
 */
@BuiltInFunction(name=ExternalSqlTypeIdFunction.NAME, args= {
    @Argument(allowedTypes= PInteger.class )} )
public class ExternalSqlTypeIdFunction extends ScalarFunction {
    public static final String NAME = "ExternalSqlTypeId";

    public ExternalSqlTypeIdFunction() {
    }

    public ExternalSqlTypeIdFunction(List<Expression> children) throws SQLException {
        super(children);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression child = children.get(0);
        if (!child.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        int sqlType = child.getDataType().getCodec().decodeInt(ptr, child.getSortOrder());
        try {
            byte[] externalIdTypeBytes = PInteger.INSTANCE.toBytes(
                PDataType.fromTypeId(sqlType).getResultSetSqlType());
            ptr.set(externalIdTypeBytes);
        } catch (IllegalDataException e) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }
    
    @Override
    public String getName() {
        return NAME;
    }
}
