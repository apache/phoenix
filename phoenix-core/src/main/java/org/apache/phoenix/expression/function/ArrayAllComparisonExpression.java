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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.types.PDataType;

public class ArrayAllComparisonExpression extends ArrayAnyComparisonExpression {

    public ArrayAllComparisonExpression() {}

    public ArrayAllComparisonExpression(List<Expression> children) {
        super(children);
    }

    @Override
    protected boolean resultFound(ImmutableBytesWritable ptr) {
        if (Bytes.equals(ptr.get(), PDataType.FALSE_BYTES)) { return true; }
        return false;
    }

    @Override
    protected boolean result() {
        return false;
    }
}
