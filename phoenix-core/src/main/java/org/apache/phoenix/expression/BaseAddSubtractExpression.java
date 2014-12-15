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
package org.apache.phoenix.expression;

import java.util.List;

import org.apache.phoenix.schema.types.PDataType;


abstract public class BaseAddSubtractExpression extends ArithmeticExpression {
    public BaseAddSubtractExpression() {
    }

    public BaseAddSubtractExpression(List<Expression> children) {
        super(children);
    }

    protected static Integer getPrecision(Integer lp, Integer rp, Integer ls, Integer rs) {
        if (ls == null || rs == null) {
            return PDataType.MAX_PRECISION;
        }
        int val = getScale(lp, rp, ls, rs) + Math.max(lp - ls, rp - rs) + 1;
        return Math.min(PDataType.MAX_PRECISION, val);
    }

    protected static Integer getScale(Integer lp, Integer rp, Integer ls, Integer rs) {
        // If we are adding a decimal with scale and precision to a decimal
        // with no precision nor scale, the scale system does not apply.
        if (ls == null || rs == null) {
            return null;
        }
        int val = Math.max(ls, rs);
        return Math.min(PDataType.MAX_PRECISION, val);
    }
}
