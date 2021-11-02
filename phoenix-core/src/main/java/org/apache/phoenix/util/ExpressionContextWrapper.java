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
package org.apache.phoenix.util;

import org.apache.phoenix.call.CallWrapper;

/**
 * CallWrapper that initializes and clears the ThreadExpressionCtx thread-local variable
 * 
 * This is the client-side equivalent to doing the same on the coprocessor side, but we cannot
 * rely on having a single context in a single client application thread, and need to switch it
 * out on every execution.
 */
public class ExpressionContextWrapper implements CallWrapper {

    private ExpressionContext context;
    private boolean wasSet = false;

    public ExpressionContextWrapper(ExpressionContext context) {
        this.context=context;
    }

    public static ExpressionContextWrapper wrap(ExpressionContext context) {
        return new ExpressionContextWrapper(context);
    }
    
    @Override
    public void before() {
        //We may call this multiple times
        if (ThreadExpressionCtx.get() == null) {
            ThreadExpressionCtx.set(context);
            wasSet = true;
        }
    }

    @Override
    public void after() {
        if (wasSet) {
            ThreadExpressionCtx.remove();
        }
    }

}
