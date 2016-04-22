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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.junit.Test;

public class BuiltinFunctionConstructorTest {

    @Test
    public void testChildrenListConstructors() {
        ExpressionType[] types = ExpressionType.values();
        List<Expression> children = new ArrayList<>();
        for(int i = 0; i < types.length; i++) {
            try {
                if((ScalarFunction.class.isAssignableFrom(types[i].getExpressionClass())) && (types[i].getExpressionClass() != UDFExpression.class)) {
                	Method cloneMethod = types[i].getExpressionClass().getMethod("clone", List.class);
                	// ScalarFunctions that implement clone(List<Expression>) don't need to implement a constructor that takes a List<Expression>  
                	if (cloneMethod==null) {
	                    Constructor cons = types[i].getExpressionClass().getDeclaredConstructor(List.class);
	                    cons.setAccessible(true);
	                    cons.newInstance(children);
                	}
                }
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testNoArgumentConstructors() {
        ExpressionType[] types = ExpressionType.values();
        for(int i = 0; i < types.length; i++) {
            try {
                if(!AggregateFunction.class.isAssignableFrom(types[i].getExpressionClass())) {
                    Constructor cons = types[i].getExpressionClass().getDeclaredConstructor();
                    cons.setAccessible(true);
                    cons.newInstance();
                }
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
            }
        }
    }
}
