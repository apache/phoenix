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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBinaryBase;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;

@BuiltInFunction(name = GetBitFunction.NAME, args = {
        @Argument(allowedTypes = { PBinary.class, PVarbinary.class }),
        @Argument(allowedTypes = { PInteger.class }) })
public class GetBitFunction extends PrefixFunction {

    public static final String NAME = "GET_BIT";

    private Integer offsetPreCompute;

    public GetBitFunction() {
    }

    public GetBitFunction(List<Expression> children) throws SQLException {
        super(children);
        init();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        // get offset parameter
        int offset;
        if (offsetPreCompute == null) {
            Expression offsetExpr = children.get(1);
            if (!offsetExpr.evaluate(tuple, ptr)) return false;
            offset = (Integer) PInteger.INSTANCE.toObject(ptr, offsetExpr.getSortOrder());
        } else offset = offsetPreCompute;
        // get binary data parameter
        Expression dataExpr = children.get(0);
        if (!dataExpr.evaluate(tuple, ptr)) return false;
        if (ptr.getLength() == 0) return true;
        int len = ptr.getLength() * Byte.SIZE;
        offset = (offset % len + len) % len;
        // set result
        ((PBinaryBase) dataExpr.getDataType()).getBit(ptr, dataExpr.getSortOrder(), offset, ptr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PInteger.INSTANCE;
    }

    private void init() {
        Expression offsetExpr = children.get(1);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        if (offsetExpr.isStateless() && offsetExpr.getDeterminism() == Determinism.ALWAYS
                && offsetExpr.evaluate(null, ptr)) {
            offsetPreCompute = (Integer) PInteger.INSTANCE.toObject(ptr, offsetExpr.getSortOrder());
        } else offsetPreCompute = null;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return (offsetPreCompute != null && offsetPreCompute == 0) ? OrderPreserving.YES_IF_LAST
                : OrderPreserving.NO;
    }
}
