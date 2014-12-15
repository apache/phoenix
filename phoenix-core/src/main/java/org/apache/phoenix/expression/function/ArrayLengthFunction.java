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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.tuple.Tuple;

@BuiltInFunction(name = ArrayLengthFunction.NAME, args = { @Argument(allowedTypes = {
		PBinaryArray.class, PVarbinary.class }) })
public class ArrayLengthFunction extends ScalarFunction {
	public static final String NAME = "ARRAY_LENGTH";

	public ArrayLengthFunction() {
	}

	public ArrayLengthFunction(List<Expression> children) {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression arrayExpr = children.get(0);
		if (!arrayExpr.evaluate(tuple, ptr)) {
			return false;
		} else if (ptr.getLength() == 0) {
			return true;
		}
		PDataType baseType = PDataType.fromTypeId(children.get(0).getDataType()
				.getSqlType()
				- PDataType.ARRAY_TYPE_BASE);
		int length = PArrayDataType.getArrayLength(ptr, baseType, arrayExpr.getMaxLength());
		byte[] lengthBuf = new byte[PInteger.INSTANCE.getByteSize()];
    PInteger.INSTANCE.getCodec().encodeInt(length, lengthBuf, 0);
		ptr.set(lengthBuf);
		return true;
	}

	@Override
	public PDataType getDataType() {
		// Array length will return an Integer
		return PInteger.INSTANCE;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
