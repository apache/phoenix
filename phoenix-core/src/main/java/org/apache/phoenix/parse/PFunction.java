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
package org.apache.phoenix.parse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.generated.PFunctionProtos;
import org.apache.phoenix.coprocessor.generated.PFunctionProtos.PFunctionArg;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PMetaDataEntity;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SizedUtil;

public class PFunction implements PMetaDataEntity {

    private PName tenantId = null;
    private final PName functionName;
    private List<FunctionArgument> args;
    private PName className;
    private PName jarPath;
    private PName returnType;
    private PTableKey functionKey;
    private long timeStamp;
    private int estimatedSize;
    private boolean temporary;
    private boolean replace;

    public PFunction(long timeStamp) { // For index delete marker
        this.timeStamp = timeStamp;
        this.args = Collections.emptyList();
        this.functionName = null;
    }

    public PFunction(String functionName, List<FunctionArgument> args, String returnType,
            String className, String jarPath) {
        this(functionName,args,returnType,className, jarPath, HConstants.LATEST_TIMESTAMP);
    }

    public PFunction(String functionName, List<FunctionArgument> args, String returnType,
            String className, String jarPath, long timeStamp) {
        this(null, functionName, args, returnType, className, jarPath, timeStamp);
    }    

    public PFunction(PName tenantId, String functionName, List<FunctionArgument> args, String returnType,
            String className, String jarPath, long timeStamp) {
        this(tenantId, functionName, args, returnType, className, jarPath, timeStamp, false);
    }
    
    public PFunction(PFunction function, boolean temporary) {
        this(function.getTenantId(), function.getFunctionName(), function.getFunctionArguments(),
                function.getReturnType(), function.getClassName(), function.getJarPath(), function
                        .getTimeStamp(), temporary, function.isReplace());
    }

    public PFunction(PFunction function, boolean temporary, boolean isReplace) {
        this(function.getTenantId(), function.getFunctionName(), function.getFunctionArguments(),
                function.getReturnType(), function.getClassName(), function.getJarPath(), function
                        .getTimeStamp(), temporary, isReplace);
    }

    public PFunction(PName tenantId, String functionName, List<FunctionArgument> args,
            String returnType, String className, String jarPath, long timeStamp, boolean temporary) {
        this(tenantId, functionName, args, returnType, className, jarPath, timeStamp, temporary,
                false);
    }

    public PFunction(PName tenantId, String functionName, List<FunctionArgument> args, String returnType,
            String className, String jarPath, long timeStamp, boolean temporary, boolean replace) {
        this.tenantId = tenantId;
        this.functionName = PNameFactory.newName(functionName);
        if (args == null){ 
            this.args = new ArrayList<FunctionArgument>();
        } else {
            this.args = args;
        }
        this.className = PNameFactory.newName(className);
        this.jarPath = jarPath == null ? null : PNameFactory.newName(jarPath);
        this.returnType = PNameFactory.newName(returnType);
        this.functionKey = new PTableKey(this.tenantId, this.functionName.getString());
        this.timeStamp = timeStamp;
        int estimatedSize = SizedUtil.OBJECT_SIZE * 2 + 23 * SizedUtil.POINTER_SIZE + 4 * SizedUtil.INT_SIZE + 2 * SizedUtil.LONG_SIZE + 2 * SizedUtil.INT_OBJECT_SIZE +
                PNameFactory.getEstimatedSize(tenantId) +
                PNameFactory.getEstimatedSize(this.functionName) +
                PNameFactory.getEstimatedSize(this.className) +
                 (jarPath==null?0:PNameFactory.getEstimatedSize(this.jarPath));
        this.temporary = temporary;
        this.replace = replace;
    }

    public PFunction(PFunction function) {
        this(function, function.isTemporaryFunction());
    }

    public String getFunctionName() {
        return functionName == null ? null : functionName.getString();
    }

    public List<FunctionArgument> getFunctionArguments() {
        return args;
    }

    public String getClassName() {
        return className.getString();
    }

    public String getJarPath() {
        return jarPath == null ? null : jarPath.getString();
    }

    public String getReturnType() {
        return returnType.getString();
    }
    
    public PTableKey getKey() {
        return this.functionKey;
    }
    
    public long getTimeStamp() {
        return this.timeStamp;
    }
    
    public PName getTenantId() {
        return this.tenantId;
    }
    
    public boolean isTemporaryFunction() {
        return temporary;
    }
    
    public static class FunctionArgument {
        private final PName argumentType;
        private final boolean isArrayType;
        private final boolean isConstant;
        private final LiteralExpression defaultValue;
        private final LiteralExpression minValue;
        private final LiteralExpression maxValue;
        private short argPosition;
        
        public FunctionArgument(String argumentType, boolean isArrayType, boolean isConstant, LiteralExpression defaultValue,
                LiteralExpression minValue, LiteralExpression maxValue) {
            this.argumentType = PNameFactory.newName(argumentType);
            this.isArrayType = isArrayType;
            this.isConstant = isConstant;
            this.defaultValue = defaultValue;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }
        public FunctionArgument(String argumentType, boolean isArrayType, boolean isConstant, LiteralExpression defaultValue,
                LiteralExpression minValue, LiteralExpression maxValue, short argPosition) {
            this(argumentType, isArrayType, isConstant, defaultValue, minValue, maxValue);
            this.argPosition = argPosition;
        }

        public String getArgumentType() {
            return argumentType.getString();
        }

        public boolean isConstant() {
            return isConstant;
        }

        public boolean isArrayType() {
            return isArrayType;
        }

        public LiteralExpression getDefaultValue() {
            return defaultValue;
        }

        public LiteralExpression getMinValue() {
            return minValue;
        }

        public LiteralExpression getMaxValue() {
            return maxValue;
        }
        
        public short getArgPosition() {
            return argPosition;
        }
    }
    
    public static PFunctionProtos.PFunction toProto(PFunction function) {
        PFunctionProtos.PFunction.Builder builder = PFunctionProtos.PFunction.newBuilder();
        if(function.getTenantId() != null){
          builder.setTenantId(ByteStringer.wrap(function.getTenantId().getBytes()));
        }
        builder.setFunctionName(function.getFunctionName());
        builder.setClassname(function.getClassName());
        if (function.getJarPath() != null) {
            builder.setJarPath(function.getJarPath());
        }
        builder.setReturnType(function.getReturnType());
        builder.setTimeStamp(function.getTimeStamp());
        for(FunctionArgument arg: function.getFunctionArguments()) {
            PFunctionProtos.PFunctionArg.Builder argBuilder = PFunctionProtos.PFunctionArg.newBuilder();
            argBuilder.setArgumentType(arg.getArgumentType());
            argBuilder.setIsArrayType(arg.isArrayType);
            argBuilder.setIsConstant(arg.isConstant);
            if(arg.getDefaultValue() != null) {
                argBuilder.setDefaultValue((String)arg.getDefaultValue().getValue());
            }
            if(arg.getMinValue() != null) {
                argBuilder.setMinValue((String)arg.getMinValue().getValue());
            }
            if(arg.getMaxValue() != null) {
                argBuilder.setMaxValue((String)arg.getMaxValue().getValue());
            }
            builder.addArguments(argBuilder.build());
        }
        if(builder.hasIsReplace()) {
            builder.setIsReplace(function.isReplace());
        }
        return builder.build();
      }

    public static PFunction createFromProto(
            org.apache.phoenix.coprocessor.generated.PFunctionProtos.PFunction function) {
        PName tenantId = null;
        if(function.hasTenantId()){
          tenantId = PNameFactory.newName(function.getTenantId().toByteArray());
        }
        String functionName = function.getFunctionName();
        long timeStamp = function.getTimeStamp();
        String className = function.getClassname();
        String jarPath = function.getJarPath();
        String returnType = function.getReturnType();
        List<FunctionArgument> args = new ArrayList<FunctionArgument>(function.getArgumentsCount());
        for(PFunctionArg arg: function.getArgumentsList()) {
            String argType = arg.getArgumentType();
            boolean isArrayType = arg.hasIsArrayType()?arg.getIsArrayType():false;
			PDataType dataType = isArrayType ? PDataType.fromTypeId(PDataType
					.sqlArrayType(SchemaUtil.normalizeIdentifier(SchemaUtil
							.normalizeIdentifier(argType)))) : PDataType
					.fromSqlTypeName(SchemaUtil.normalizeIdentifier(argType));
            boolean isConstant = arg.hasIsConstant()?arg.getIsConstant():false;
            String defaultValue = arg.hasDefaultValue()?arg.getDefaultValue():null;
            String minValue = arg.hasMinValue()?arg.getMinValue():null;
            String maxValue = arg.hasMaxValue()?arg.getMaxValue():null;
            args.add(new FunctionArgument(argType, isArrayType, isConstant,
                    defaultValue == null ? null : LiteralExpression.newConstant((new LiteralParseNode(dataType.toObject(defaultValue))).getValue()),
                    minValue == null ? null : LiteralExpression.newConstant((new LiteralParseNode(dataType.toObject(minValue))).getValue()),
                    maxValue == null ? null : LiteralExpression.newConstant((new LiteralParseNode(dataType.toObject(maxValue))).getValue())));
        }
        return new PFunction(tenantId, functionName, args, returnType, className, jarPath,
                timeStamp, false, function.hasIsReplace() ? true : false);
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

    public boolean isReplace() {
        return this.replace;
    }
}

