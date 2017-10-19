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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.text.Collator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.CollationKeyFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * "Unit" tests for CollationKeyFunction
 * 
 * @author snakhoda-sfdc
 *
 */
public class CollationKeyFunctionTest {

	@Test
	public void testChineseCollationKeyBytes() throws Exception {

		// Chinese (China)
		test("\u963f", "zh", "02eb000000010000");
		test("\u55c4", "zh", "14ad000000010000");
		test("\u963e", "zh", "8000963f0000000100010000");
		test("\u554a", "zh", "02ea000000010000");
		test("\u4ec8", "zh", "80004ec90000000100010000");
		test("\u3d9a", "zh", "80003d9b0000000100010000");
		test("\u9f51", "zh", "1905000000010000");

		// Chinese (Taiwan)
		test("\u963f", "zh_TW", "063d000000010000");
		test("\u55c4", "zh_TW", "241e000000010000");
		test("\u963e", "zh_TW", "8000963f0000000100010000");
		test("\u554a", "zh_TW", "09c9000000010000");
		test("\u4ec8", "zh_TW", "181b000000010000");
		test("\u3d9a", "zh_TW", "80003d9b0000000100010000");
		test("\u9f51", "zh_TW", "80009f520000000100010000");

		// Chinese (Taiwan, Stroke)
		test("\u963f", "zh_TW_STROKE", "5450010500");
		test("\u55c4", "zh_TW_STROKE", "7334010500");
		test("\u963e", "zh_TW_STROKE", "544f010500");
		test("\u554a", "zh_TW_STROKE", "62de010500");
		test("\u4ec8", "zh_TW_STROKE", "46be010500");
		test("\u3d9a", "zh_TW_STROKE", "a50392010500");
		test("\u9f51", "zh_TW_STROKE", "8915010500");

		// Chinese (China, Stroke)
		test("\u963f", "zh__STROKE", "28010500");
		test("\u55c4", "zh__STROKE", "2a010500");
		test("\u963e", "zh__STROKE", "7575010500");
		test("\u554a", "zh__STROKE", "2b010500");
		test("\u4ec8", "zh__STROKE", "51a1010500");
		test("\u3d9a", "zh__STROKE", "a50392010500");
		test("\u9f51", "zh__STROKE", "6935010500");

		// Chinese (China, Pinyin)
		test("\u963f", "zh__PINYIN", "28010500");
		test("\u55c4", "zh__PINYIN", "2a010500");
		test("\u963e", "zh__PINYIN", "7575010500");
		test("\u554a", "zh__PINYIN", "2b010500");
		test("\u4ec8", "zh__PINYIN", "51a1010500");
		test("\u3d9a", "zh__PINYIN", "a50392010500");
		test("\u9f51", "zh__PINYIN", "6935010500");
	}	
	
	@Test
	public void testUpperCaseCollationKeyBytes() throws Exception {
		String inputStrLowerCase = "abcdef";
		String inputStrUpperCase = "ABCDEF";
		String inputStrMixedCase = "aBcDeF";
		String expectedCollationKeyBytes = "00530054005500560058005900000001000100010001000100010000";

		// We expect the same collation key bytes for upper, lower and mixed-case versions of the input
		test(inputStrLowerCase, "en", Boolean.TRUE, null, null, expectedCollationKeyBytes);
		test(inputStrUpperCase, "en", Boolean.TRUE, null, null, expectedCollationKeyBytes);
		test(inputStrMixedCase, "en", Boolean.TRUE, null, null, expectedCollationKeyBytes);
	}

	@Test
	public void testCollationKeyBytesForPrimaryStrength() throws Exception {
		// "a", "A", "ä" are considered equivalent
		testMultiple(ImmutableMap.of(
				"a", "005300000000",
				"b", "005400000000",
				"ä", "005300000000",
				"A", "005300000000"),
				"en", Boolean.FALSE, Collator.PRIMARY, null);

	}
	
	@Test
	public void testCollationKeyBytesForSecondaryStrength() throws Exception {
		// "a" and "A" are considered equivalent but not "ä"
		testMultiple(ImmutableMap.of(
				"a", "0053000000010000",
				"b", "0054000000010000",
				"ä", "00530000000100900000",
				"A", "0053000000010000"),
				"en", Boolean.FALSE, Collator.SECONDARY, null);		
	}
	
	@Test
	public void testCollationKeyBytesForTertiaryStrength() throws Exception {
		// none of these are considered equivalent
		testMultiple(ImmutableMap.of(
				"a", "00530000000100000001",
				"b", "00540000000100000001",
				"ä", "005300000001009000000001",
				"A", "00530000000100000002"),
				"en", Boolean.FALSE, Collator.TERTIARY, null);				
	}

	@Test
	public void testCollationKeyBytesForNoDecomposition() throws Exception {
		test("ác´¸", "en", Boolean.FALSE, null, Collator.NO_DECOMPOSITION, "00530055000d00130000000100890001000100010000");
	}
	
	@Test
	public void testCollationKeyBytesForFullDecomposition() throws Exception {
		// accented characters are affected by decomposition
		test("ác´¸", "en", Boolean.FALSE, null, Collator.FULL_DECOMPOSITION, "00530055000000010089000100770089007700960000");
	}
	
	// Helper methods
	private static void testMultiple(Map<String, String> expectedCollationKeyBytesHexMap, String localeIsoCode, Boolean upperCaseCollator, Integer strength, 
			Integer decomposition) throws Exception {
		for(Map.Entry<String, String> mapEntry: expectedCollationKeyBytesHexMap.entrySet()) {
			test(mapEntry.getKey(), localeIsoCode, upperCaseCollator, strength, decomposition, mapEntry.getValue());
		}
	}
	
	private static void test(String inputStr, String localeIsoCode, Boolean upperCaseCollator, Integer strength, 
			Integer decomposition, String expectedCollationKeyBytesHex) throws Exception {
		testExpression(inputStr, localeIsoCode, SortOrder.ASC, upperCaseCollator, strength, decomposition, expectedCollationKeyBytesHex);
		testExpression(inputStr, localeIsoCode, SortOrder.DESC, upperCaseCollator, strength, decomposition, expectedCollationKeyBytesHex);
	}

	private static void test(String inputStr, String localeIsoCode, String expectedCollationKeyBytesHex)
			throws Exception {
		testExpression(inputStr, localeIsoCode, SortOrder.ASC, expectedCollationKeyBytesHex);
		testExpression(inputStr, localeIsoCode, SortOrder.DESC, expectedCollationKeyBytesHex);
	}

	private static void testExpression(String inputStr, String localeIsoCode, SortOrder sortOrder,
			Boolean upperCaseCollator, Integer strength, Integer decomposition, String expectedCollationKeyBytesHex)
			throws Exception {
		LiteralExpression inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral;
		inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);
		localeIsoCodeLiteral = LiteralExpression.newConstant(localeIsoCode, PVarchar.INSTANCE, sortOrder);
		upperCaseBooleanLiteral = LiteralExpression.newConstant(upperCaseCollator, PBoolean.INSTANCE, sortOrder);
		strengthLiteral = LiteralExpression.newConstant(strength, PInteger.INSTANCE, sortOrder);
		decompositionLiteral = LiteralExpression.newConstant(decomposition, PInteger.INSTANCE, sortOrder);
		testExpression(inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral, expectedCollationKeyBytesHex);
	}

	private static void testExpression(String inputStr, String localeIsoCode, SortOrder sortOrder,
			String expectedCollationKeyBytesHex) throws Exception {
		testExpression(inputStr, localeIsoCode, sortOrder,  Boolean.FALSE, null,
				null, expectedCollationKeyBytesHex);
	}

	private static void testExpression(LiteralExpression inputStrLiteral, LiteralExpression localeIsoCodeLiteral,
			LiteralExpression upperCaseBooleanLiteral, LiteralExpression strengthLiteral,
			LiteralExpression decompositionLiteral, String expectedCollationKeyBytesHex) throws Exception {
		List<Expression> expressions = Lists.newArrayList((Expression) inputStrLiteral,
				(Expression) localeIsoCodeLiteral, (Expression) upperCaseBooleanLiteral, (Expression) strengthLiteral,
				(Expression) decompositionLiteral);
		Expression collationKeyFunction = new CollationKeyFunction(expressions);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		boolean ret = collationKeyFunction.evaluate(null, ptr);
		if (ret) {
			byte[] result = (byte[]) collationKeyFunction.getDataType().toObject(ptr,
					collationKeyFunction.getSortOrder());

			byte[] expectedCollationKeyByteArray = Hex.decodeHex(expectedCollationKeyBytesHex.toCharArray());

			assertArrayEquals("COLLATION_KEY did not return the expected byte array", expectedCollationKeyByteArray,
					result);

		} else {
			fail(String.format("CollationKeyFunction.evaluate returned false for %s, %s", inputStrLiteral.getValue(),
					localeIsoCodeLiteral.getValue()));
		}
	}
}
