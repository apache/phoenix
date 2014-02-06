/*
 * Copyright 2014 The Apache Software Foundation
 *
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

import java.math.MathContext;
import java.math.RoundingMode;


public class FloorDecimalExpression extends CeilingDecimalExpression {
    private static final MathContext FLOOR_CONTEXT = new MathContext(0, RoundingMode.FLOOR);
    
    public FloorDecimalExpression() {
    }
    
    public FloorDecimalExpression(Expression child) {
        super(child);
    }
    
    @Override
    protected MathContext getMathContext() {
        return FLOOR_CONTEXT;
    }
    
    
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder("FLOOR(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(getChild().toString());
        }
        buf.append(")");
        return buf.toString();
    }
}
