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
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.i18n.LocaleUtils;

@FunctionParseNode.BuiltInFunction(name=LowerFunction.NAME,  args={
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class}),
        @FunctionParseNode.Argument(allowedTypes={PVarchar.class}, defaultValue="null", isConstant=true)} )
public class LowerFunction extends ScalarFunction {
    public static final String NAME = "LOWER";

    private Locale locale = null;

    public LowerFunction() {
    }

    public LowerFunction(List<Expression> children) throws SQLException {
        super(children);
        initialize();
    }

    private void initialize() {
        if (children.size() > 1) {
            String localeISOCode = getLiteralValue(1, String.class);
            locale = LocaleUtils.get().getLocaleByIsoCode(localeISOCode);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        initialize();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getStrExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength()==0) {
            return true;
        }

        String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr, getStrExpression().getSortOrder());

        if (sourceStr == null) {
            return true;
        }

        String resultStr = locale == null ? sourceStr.toLowerCase() : sourceStr.toLowerCase(locale);

        ptr.set(PVarchar.INSTANCE.toBytes(resultStr));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return getStrExpression().getDataType();
    }

    @Override
    public Integer getMaxLength() {
        return getStrExpression().getMaxLength();
    }

    @Override
    public boolean isNullable() {
        return getStrExpression().isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getStrExpression() {
        return children.get(0);
    }
}
