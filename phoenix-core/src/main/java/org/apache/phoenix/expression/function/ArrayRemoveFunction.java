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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarbinaryArray;
import org.apache.phoenix.schema.types.PhoenixArray;

@FunctionParseNode.BuiltInFunction(name = ArrayRemoveFunction.NAME, args = {
		@FunctionParseNode.Argument(allowedTypes = { PBinaryArray.class, PVarbinaryArray.class }),
		@FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }, defaultValue = "null") })
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

		PhoenixArray array = (PhoenixArray) getDataType().toObject(arrayBytes, offset, length, getDataType(), sortOrder,
				maxLength, null);

		
		Object toCompare = baseType.toObject(ptr, sortOrder);
		
		if(baseType.isFixedWidth()) {
			toCompare = baseType.pad(toCompare, array.getMaxLength());
		}

		int dimensions = array.getDimensions();
		List<Object> values = new ArrayList<>();
		for (int i = 0; i < dimensions; i++) {
			Object element = array.getElement(i);
			if (element != null && element.equals(toCompare)
					|| (element.getClass().isArray() && ArrayUtils.isEquals(element, toCompare))) {
				if (getDataType().isFixedWidth()) {
					values.add(null);
				}
			} else {
				values.add(element);
			}
		}

		PhoenixArray newArray = PArrayDataType.instantiatePhoenixArray(baseType,
				values.toArray(new Object[values.size()]));

		if (baseType.isFixedWidth() && maxLength != null && maxLength != newArray.getMaxLength()) {
			newArray = new PhoenixArray(newArray, maxLength);
		}

		ptr.set(getDataType().toBytes(newArray, sortOrder));

		return true;
	}

	@Override
	public String getName() {
		return NAME;
	}

}
