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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.util.ByteUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;


@FunctionParseNode.BuiltInFunction(name=ArrayToJsonFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PBinaryArray.class, PVarbinaryArray.class})})
public class ArrayToJsonFunction extends ScalarFunction {
    public static final String NAME = "ARRAY_TO_JSON";

    public ArrayToJsonFunction() {
    }

    public ArrayToJsonFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arrayExpr = getChildren().get(0);

        if (!arrayExpr.evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }
        try {
            PDataType baseType = PDataType.fromTypeId(arrayExpr.getDataType()
                    .getSqlType()
                    - PDataType.ARRAY_TYPE_BASE);
            int length = PArrayDataType.getArrayLength(ptr, baseType, arrayExpr.getMaxLength());
            StringBuilder builder = new StringBuilder("[");
            for(int i=1;i<=length;i++){
                ImmutableBytesWritable tmp = new ImmutableBytesWritable(ptr.get(),ptr.getOffset(),ptr.getLength());
                PArrayDataType.positionAtArrayElement(tmp, i - 1,baseType, arrayExpr.getMaxLength());
                Object re = baseType.toObject(tmp.get(),tmp.getOffset(),tmp.getLength());
                builder.append(PhoenixJson.dataToJsonValue(baseType, re));
                if(i != length)
                    builder.append(",");
            }
            builder.append("]");
            String str = builder.toString();
            PhoenixJson phoenixJson = PhoenixJson.getInstance(str);
            byte[] json = PJson.INSTANCE.toBytes(phoenixJson);
            ptr.set(json);
        } catch (SQLException sqe) {
            new IllegalDataException(sqe);
        }
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public PDataType getDataType() {
        return PJson.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
