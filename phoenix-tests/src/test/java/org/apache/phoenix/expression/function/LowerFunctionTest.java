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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * "Unit" tests for LowerFunction
 * 
 */
public class LowerFunctionTest {

	
	// These maps were obtained from Java API docs for java.lang.String
	// https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#toLowerCase-java.util.Locale-
	private static ImmutableMap<String, String> turkishLowerToUpperCaseMap = 
			ImmutableMap.of("\u0130", "\u0069", 
			                "\u0049", "\u0131");
			
	private static ImmutableMap<String, String> anyLocaleLowerToUpperCaseMap = 
			ImmutableMap.of( "\u0399\u03a7\u0398\u03a5\u03a3", "\u03b9\u03c7\u03b8\u03c5\u03c2", 
			                // IXΘϒΣ -> ιχθυς (the last character is the "lunate sigma")
					         "FrEnCh Fries", "french fries");

	@Test
	public void testTurkishUpperCase() throws Exception {
		testLowerToUpperCaseMap(turkishLowerToUpperCaseMap, "tr");
	}
	
	@Test
	public void testUniversalUpperCaseNoLocale() throws Exception {
		testLowerToUpperCaseMap(anyLocaleLowerToUpperCaseMap, null);
	}
	
	@Test
	public void testUniversalUpperCaseTurkish() throws Exception {
		testLowerToUpperCaseMap(anyLocaleLowerToUpperCaseMap, "tr");
	}
	
	private void testLowerToUpperCaseMap(Map<String, String> lowerToUpperMap, String locale) throws Exception {
		for(Map.Entry<String, String> lowerUpperPair: lowerToUpperMap.entrySet()) {
			String upperCaseResultAsc = callFunction(lowerUpperPair.getKey(), locale, SortOrder.ASC);
			String upperCaseResultDesc = callFunction(lowerUpperPair.getKey(), locale, SortOrder.DESC);
			
			assertEquals("Result of calling LowerFunction[ASC] on [" + lowerUpperPair.getKey() + "][" + locale + "] not as expected.",
			  lowerUpperPair.getValue(), upperCaseResultAsc);
			assertEquals("Result of calling LowerFunction[DESC] on [" + lowerUpperPair.getKey() + "][" + locale + "] not as expected.",
					  lowerUpperPair.getValue(), upperCaseResultDesc);
		}
	}

	private static String callFunction(String inputStr, String localeIsoCode, SortOrder sortOrder) throws Exception {
		LiteralExpression inputStrLiteral, localeIsoCodeLiteral;
		inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);
		localeIsoCodeLiteral = LiteralExpression.newConstant(localeIsoCode, PVarchar.INSTANCE, sortOrder);
		List<Expression> expressions = Lists.newArrayList((Expression) inputStrLiteral,
				(Expression) localeIsoCodeLiteral);
		Expression lowerFunction = new LowerFunction(expressions);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		boolean ret = lowerFunction.evaluate(null, ptr);
		String result = ret
				? (String) lowerFunction.getDataType().toObject(ptr, lowerFunction.getSortOrder()) : null;
		return result;
	}
}
