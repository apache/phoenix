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
 * "Unit" tests for UpperFunction
 * 
 */
public class UpperFunctionTest {

	
	// These maps were obtained from Java API docs for java.lang.String
	// https://docs.oracle.com/javase/8/docs/api/java/lang/String.html#toUpperCase-java.util.Locale-
	private static ImmutableMap<String, String> turkishLowerToUpperCaseMap = 
			ImmutableMap.of("\u0069", "\u0130",
					        "\u0131", "\u0049");
			
	private static ImmutableMap<String, String> anyLocaleLowerToUpperCaseMap = 
			ImmutableMap.of("\u00df", "\u0053\u0053",
					         "Fahrvergnügen", "FAHRVERGNÜGEN");

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
			
			assertEquals("Result of calling UpperFunction[ASC] on [" + lowerUpperPair.getKey() + "][" + locale + "] not as expected.",
			  lowerUpperPair.getValue(), upperCaseResultAsc);
			assertEquals("Result of calling UpperFunction[DESC] on [" + lowerUpperPair.getKey() + "][" + locale + "] not as expected.",
					  lowerUpperPair.getValue(), upperCaseResultDesc);
		}
	}

	private static String callFunction(String inputStr, String localeIsoCode, SortOrder sortOrder) throws Exception {
		LiteralExpression inputStrLiteral, localeIsoCodeLiteral;
		inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);
		localeIsoCodeLiteral = LiteralExpression.newConstant(localeIsoCode, PVarchar.INSTANCE, sortOrder);
		List<Expression> expressions = Lists.newArrayList((Expression) inputStrLiteral,
				(Expression) localeIsoCodeLiteral);
		Expression upperFunction = new UpperFunction(expressions);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		boolean ret = upperFunction.evaluate(null, ptr);
		String result = ret
				? (String) upperFunction.getDataType().toObject(ptr, upperFunction.getSortOrder()) : null;
		return result;
	}
}
