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

import java.text.FieldPosition;
import java.text.Format;
import java.text.ParsePosition;

import org.apache.commons.codec.binary.Hex;

/**
 * A formatter that formats a byte array to a hexadecimal string
 * (with each byte converted to a 2-digit hex sequence)
 *
 * @author snakhoda-sfdc
 */
public class VarBinaryFormatter extends Format {

	private static final long serialVersionUID = -7940880118392024750L;

	public static final VarBinaryFormatter INSTANCE = new VarBinaryFormatter();

	@Override
	public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
		if (!(obj instanceof byte[])) {
			throw new IllegalArgumentException("VarBinaryFormatter can only format byte arrays");
		}
		String hexString = Hex.encodeHexString((byte[]) obj);
		toAppendTo.append(hexString);
		return toAppendTo;
	}

	@Override
	public Object parseObject(String source, ParsePosition pos) {
		return new UnsupportedOperationException();
	}
}
