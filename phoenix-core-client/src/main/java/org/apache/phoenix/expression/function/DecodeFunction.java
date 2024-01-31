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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Convert string to bytes
 */
@FunctionParseNode.BuiltInFunction(name = DecodeFunction.NAME, args = {
	@FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
	@FunctionParseNode.Argument(enumeration = "EncodeFormat")})
public class DecodeFunction extends ScalarFunction {

	public static final String NAME = "DECODE";

	public DecodeFunction() {
	}

	public DecodeFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression expression = getExpression();
		if (!expression.evaluate(tuple, ptr)) {
			return false;
		}
		if (ptr.getLength() == 0) {
			return true; // expression was evaluated, but evaluated to null
		}

		PDataType type = expression.getDataType();
		String stringToDecode = (String) type.toObject(ptr);

		Expression encodingExpression = getEncodingExpression();
		if (!encodingExpression.evaluate(tuple, ptr)) {
			return false;
		}

		if (ptr.getLength() == 0) {
	        throw new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
	        .setMessage("Missing bytes encoding").build().buildException());
		}

		type = encodingExpression.getDataType();
		String encoding = ((String) type.toObject(ptr)).toUpperCase();

		byte out[];

		EncodeFormat format = EncodeFormat.valueOf(encoding);
		switch (format) {
			case HEX:
				out = decodeHex(stringToDecode);
				break;
			default:
				throw new IllegalDataException("Unsupported encoding \"" + encoding + "\"");
		}
		ptr.set(out);

		return true;
	}

	private byte[] decodeHex(String hexStr) {
		byte[] out = new byte[hexStr.length() / 2];
		for (int i = 0; i < hexStr.length(); i = i + 2) {
			try {
				out[i / 2] = (byte) Integer.parseInt(hexStr.substring(i, i + 2), 16);
			} catch (NumberFormatException ex) {
				throw new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
		        .setMessage("Value " + hexStr.substring(i, i + 2) + " cannot be cast to hex number").build().buildException());
			} catch (StringIndexOutOfBoundsException ex) {
				throw new IllegalDataException(new SQLExceptionInfo.Builder(SQLExceptionCode.ILLEGAL_DATA)
                .setMessage("Invalid value length, cannot cast to hex number (" + hexStr + ")").build().buildException());
			}
		}
		return out;
	}

	@Override
	public PDataType getDataType() {
		return PVarbinary.INSTANCE;
	}

	@Override
	public boolean isNullable() {
		return getExpression().isNullable();
	}

	private Expression getExpression() {
		return children.get(0);
	}

	private Expression getEncodingExpression() {
		return children.get(1);
	}

	@Override
	public String getName() {
		return NAME;
	}

	@Override
	public Integer getMaxLength() {
		return getExpression().getMaxLength();
	}
}
