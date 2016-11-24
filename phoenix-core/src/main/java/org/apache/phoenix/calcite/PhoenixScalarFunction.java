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
package org.apache.phoenix.calcite;

import java.util.List;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexImpTable.NullAs;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.parse.FunctionParseNode.FunctionClassType;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PFunction.FunctionArgument;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class PhoenixScalarFunction implements ScalarFunction, ImplementableFunction {
    private final PFunction pFunction;
    @SuppressWarnings("rawtypes")
    private final PDataType returnType;
    private final List<FunctionParameter> parameters;
    private final BuiltInFunctionInfo builtInFunction;

    public PhoenixScalarFunction(PFunction functionInfo) {
        this.pFunction = functionInfo;
        this.returnType =
                PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(functionInfo.getReturnType()));
        this.parameters = Lists.newArrayListWithExpectedSize(functionInfo.getFunctionArguments().size());
        for (final FunctionArgument arg : functionInfo.getFunctionArguments()) {
            parameters.add(
                    new FunctionParameter() {
                        public int getOrdinal() {
                            return arg.getArgPosition();
                        }

                        public String getName() {
                            return getArgumentName(arg.getArgPosition());
                        }

                        @SuppressWarnings("rawtypes")
                        public RelDataType getType(RelDataTypeFactory typeFactory) {
                            PDataType dataType =
                                    arg.isArrayType() ? PDataType.fromTypeId(PDataType.sqlArrayType(SchemaUtil
                                            .normalizeIdentifier(SchemaUtil.normalizeIdentifier(arg
                                                    .getArgumentType())))) : PDataType.fromSqlTypeName(SchemaUtil
                                            .normalizeIdentifier(arg.getArgumentType()));
                            return typeFactory.createJavaType(dataType.getJavaClass());
                        }

                        public boolean isOptional() {
                            return arg.getDefaultValue() != null;
                        }
                    });
        }
        this.builtInFunction = null;
    }

    public PhoenixScalarFunction(BuiltInFunctionInfo info, List<FunctionParameter> parameters, PDataType returnType){
        this.builtInFunction = info;
        this.parameters = parameters;
        this.returnType = returnType;
        this.pFunction = null;
    }

    private static PDataType evaluateReturnType(Class<? extends FunctionExpression> f, List<FunctionArgument> argumentList) {
        BuiltInFunction d = f.getAnnotation(BuiltInFunction.class);
        try {
            // Direct evaluation of the return type
            FunctionExpression func = f.newInstance();
            return func.getDataType();
        } catch (Exception e) {
            // should never happen
            if (d.classType() == FunctionClassType.ALIAS || d.classType() == FunctionClassType.ABSTRACT) {
                throw new RuntimeException();
            }
            // Grab the primary argument
            assert(argumentList.size() != 0);
            return PDataType.fromSqlTypeName(argumentList.get(0).getArgumentType());
        }
    }

    public static List<PhoenixScalarFunction> createBuiltinFunctions(BuiltInFunctionInfo functionInfo){
        List<List<FunctionArgument>> overloadedArgs = PhoenixSchema.overloadArguments(functionInfo.getArgs());
        List<PhoenixScalarFunction> functionList = Lists.newArrayListWithExpectedSize(overloadedArgs.size());
        Class<? extends FunctionExpression> clazz = functionInfo.getFunc();

        try {
            for (List<FunctionArgument> argumentList : overloadedArgs) {
                List<FunctionParameter> parameters = Lists.newArrayListWithExpectedSize(argumentList.size());
                PDataType returnType = evaluateReturnType(clazz, argumentList);

                for (final FunctionArgument arg : argumentList) {
                    parameters.add(
                            new FunctionParameter() {
                                public int getOrdinal() {
                                    return arg.getArgPosition();
                                }

                                public String getName() {
                                    return getArgumentName(arg.getArgPosition());
                                }

                                @SuppressWarnings("rawtypes")
                                public RelDataType getType(RelDataTypeFactory typeFactory) {
                                    PDataType dataType =
                                            arg.isArrayType() ? PDataType.fromTypeId(PDataType.sqlArrayType(SchemaUtil
                                                    .normalizeIdentifier(SchemaUtil.normalizeIdentifier(arg
                                                            .getArgumentType())))) : PDataType.fromSqlTypeName(SchemaUtil
                                                    .normalizeIdentifier(arg.getArgumentType()));
                                    return typeFactory.createJavaType(dataType.getJavaClass());
                                }

                                public boolean isOptional() {
                                    return arg.getDefaultValue() != null;
                                }
                            });
                }
                functionList.add(new PhoenixScalarFunction(functionInfo, parameters, returnType));
            }
        } catch (Exception e){
            throw new RuntimeException("Builtin function " + functionInfo.getName() + " could not be registered", e);
        }
        return functionList;
    }
    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(returnType.getJavaClass());
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return parameters;
    }
    
    public PFunction getPFunction() {
        return pFunction;
    }

    public BuiltInFunctionInfo getBuiltInFunction(){
        return builtInFunction;
    }

    private static String getArgumentName(int ordinal) {
        return "arg" + ordinal;
    }

    @Override
    public CallImplementor getImplementor() {
        return new CallImplementor() {
            public Expression implement(RexToLixTranslator translator, RexCall call, NullAs nullAs) {
                return Expressions.constant(null);
            }
        };
    }
}