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
import org.apache.phoenix.parse.ArrayModifierParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PArrayDataTypeDecoder;
import org.apache.phoenix.schema.types.PArrayDataTypeEncoder;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryArray;
import org.apache.phoenix.util.StringUtil;

@FunctionParseNode.BuiltInFunction(name = ArrayRemoveFunction.NAME, nodeClass = ArrayModifierParseNode.class, args = {
		@FunctionParseNode.Argument(allowedTypes = { PBinaryArray.class, PVarbinaryArray.class }),
		@FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }) })
public class ArrayRemoveFunction extends ArrayModifierFunction {

	public static final String NAME = "ARRAY_REMOVE";

	public ArrayRemoveFunction() {
	}

	public ArrayRemoveFunction(List<Expression> children) throws TypeMismatchException {
		super(children);
	}

	@Override
	protected boolean modifierFunction(ImmutableBytesWritable ptr, int length, int offset, byte[] arrayBytes,
			PDataType baseType, int arrayLength, Integer maxLength, Expression arrayExp) {
		SortOrder sortOrder = arrayExp.getSortOrder();

		if (ptr.getLength() == 0 || arrayBytes.length == 0) {
			ptr.set(arrayBytes, offset, length);
			return true;
		}

		PArrayDataTypeEncoder arrayDataTypeEncoder = new PArrayDataTypeEncoder(baseType, sortOrder);

		if (getRHSBaseType().equals(PChar.INSTANCE)) {
			int unpaddedCharLength = StringUtil.getUnpaddedCharLength(ptr.get(), ptr.getOffset(), ptr.getLength(),
					sortOrder);
			ptr.set(ptr.get(), offset, unpaddedCharLength);
		}

		for (int arrayIndex = 0; arrayIndex < arrayLength; arrayIndex++) {
			ImmutableBytesWritable ptr2 = new ImmutableBytesWritable(arrayBytes, offset, length);
			PArrayDataTypeDecoder.positionAtArrayElement(ptr2, arrayIndex, baseType, maxLength);
			if (baseType.compareTo(ptr2, sortOrder, ptr, sortOrder, baseType) != 0) {
				arrayDataTypeEncoder.appendValue(ptr2.get(), ptr2.getOffset(), ptr2.getLength());
			}
		}

		ptr.set(arrayDataTypeEncoder.encode());

		return true;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
