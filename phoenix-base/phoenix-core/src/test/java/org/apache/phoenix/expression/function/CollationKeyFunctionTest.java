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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.text.Collator;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.primitives.UnsignedBytes;

/**
 * "Unit" tests for CollationKeyFunction
 * 
 */
public class CollationKeyFunctionTest {

	private static String[] chineseChars = new String[] { "\u963f", "\u55c4", "\u963e", "\u554a", "\u4ec8", "\u3d9a",
			"\u9f51" };

	private static Comparator<byte[]> collationKeyComparator = UnsignedBytes.lexicographicalComparator();

	private static Comparator<ByteArrayAndInteger> collationKeyAndIndexComparator = new Comparator<ByteArrayAndInteger>() {
		@Override
		public int compare(ByteArrayAndInteger o1, ByteArrayAndInteger o2) {
			int compareResult = collationKeyComparator.compare(o1.byteArray, o2.byteArray);
			if (compareResult == 0) {
				compareResult = o1.integer.compareTo(o2.integer);
			}
			return compareResult;
		}
	};

	private static class ByteArrayAndInteger {

		private ByteArrayAndInteger(byte[] byteArray, Integer integer) {
			super();
			this.byteArray = byteArray;
			this.integer = integer;
		}

		byte[] byteArray;
		Integer integer;

		public String toString() {
			return ToStringBuilder.reflectionToString(this);
		}

		public static ByteArrayAndInteger findFirstIntegerMatch(List<ByteArrayAndInteger> list,
				Integer matchingInteger) {
			for (ByteArrayAndInteger entry : list) {
				if (entry.integer.equals(matchingInteger)) {
					return entry;
				}
			}
			return null;
		}
	}

	@Test
	public void testZhSort() throws Exception {
		testSortOrderNoEquals(chineseChars, "zh", Boolean.FALSE, null, null, new Integer[] { 4, 3, 1, 5, 2, 0, 6 });

	}

	@Test
	public void testZhTwSort() throws Exception {
		testSortOrderNoEquals(chineseChars, "zh_TW", Boolean.FALSE, null, null, new Integer[] { 4, 3, 1, 5, 2, 0, 6 });
	}

	@Test
	public void testZhTwStrokeSort() throws Exception {
		testSortOrderNoEquals(chineseChars, "zh_TW_STROKE", Boolean.FALSE, null, null,
				new Integer[] { 4, 2, 0, 3, 1, 6, 5 });
	}

	@Test
	public void testZhStrokeSort() throws Exception {
		testSortOrderNoEquals(chineseChars, "zh__STROKE", Boolean.FALSE, null, null,
				new Integer[] { 4, 2, 0, 3, 1, 6, 5 });
	}

	@Test
	public void testZhPinyinSort() throws Exception {
		testSortOrderNoEquals(chineseChars, "zh__PINYIN", Boolean.FALSE, null, null,
				new Integer[] { 0, 1, 3, 4, 6, 2, 5 });
	}

	@Test
	public void testUpperCaseCollationKeyBytes() throws Exception {
		testCollationKeysEqual(new String[] { "abcdef", "ABCDEF", "aBcDeF" }, "en", Boolean.TRUE, null, null);
	}
	
	@Test
	public void testNullCollationKey() throws Exception {		
		List<ByteArrayAndInteger> collationKeys = calculateCollationKeys(new String[] { null }, "en", null, null, null);
		assertNull(collationKeys.get(0).byteArray);
	}

	@Test
	public void testEqualCollationKeysForPrimaryStrength() throws Exception {
		// "a", "A", "ä" are considered equivalent
		testCollationKeysEqual(new String[] { "a", "A", "ä" }, "en", Boolean.FALSE, Collator.PRIMARY, null);
		testSortOrderNoEquals(new String[] { "b", "a" }, "en", Boolean.FALSE, Collator.PRIMARY, null,
				new Integer[] { 1, 0 });

	}

	@Test
	public void testCollationKeyBytesForSecondaryStrength() throws Exception {
		// "a" and "A" are considered equivalent but not "ä"
		testCollationKeysEqual(new String[] { "a", "A" }, "en", Boolean.FALSE, Collator.SECONDARY, null);
		testSortOrderNoEquals(new String[] { "b", "a", "ä" }, "en", Boolean.FALSE, Collator.SECONDARY, null,
				new Integer[] { 1, 2, 0 });
	}

	@Test
	public void testCollationKeyBytesForTertiaryStrength() throws Exception {
		// none of these are considered equivalent
		testSortOrderNoEquals(new String[] { "b", "a", "ä", "A" }, "en", Boolean.FALSE, Collator.TERTIARY, null,
				new Integer[] { 1, 3, 2, 0 });
	}

	/**
	 * Just test that changing the decomposition mode works for basic sorting.
	 * TODO: Actually test for the accented characters and languages where this
	 * actually matters.
	 */
	@Test
	public void testCollationKeyBytesForFullDecomposition() throws Exception {
		testCollationKeysEqual(new String[] { "a", "A" }, "en", Boolean.FALSE, null, Collator.FULL_DECOMPOSITION);
	}

	/** HELPER METHODS **/
	private void testSortOrderNoEquals(String[] inputStrings, String locale, Boolean uppercaseCollator,
			Integer strength, Integer decomposition, Integer[] expectedOrder) throws Exception {
		List<ByteArrayAndInteger> sortedCollationKeysAndIndexes = calculateCollationKeys(inputStrings, locale,
				uppercaseCollator, strength, decomposition);
		Collections.sort(sortedCollationKeysAndIndexes, collationKeyAndIndexComparator);
		testCollationKeysNotEqual(inputStrings, sortedCollationKeysAndIndexes);

		Integer[] sortedIndexes = new Integer[sortedCollationKeysAndIndexes.size()];
		for (int i = 0; i < sortedIndexes.length; i++) {
			sortedIndexes[i] = sortedCollationKeysAndIndexes.get(i).integer;
		}
		assertArrayEquals(expectedOrder, sortedIndexes);
	}

	private List<ByteArrayAndInteger> calculateCollationKeys(String[] inputStrings, String locale,
			Boolean upperCaseCollator, Integer strength, Integer decomposition) throws Exception {
		List<ByteArrayAndInteger> collationKeysAndIndexes = Lists.newArrayList();
		for (int i = 0; i < inputStrings.length; i++) {
			byte[] thisCollationKeyBytes = callFunction(inputStrings[i], locale, upperCaseCollator, strength,
					decomposition, SortOrder.ASC);
			collationKeysAndIndexes.add(new ByteArrayAndInteger(thisCollationKeyBytes, i));
		}
		return collationKeysAndIndexes;
	}

	private void testCollationKeysEqual(String[] inputStrings, String locale, Boolean upperCaseCollator,
			Integer strength, Integer decomposition) throws Exception {
		List<ByteArrayAndInteger> collationKeysAndIndexes = calculateCollationKeys(inputStrings, locale,
				upperCaseCollator, strength, decomposition);

		for (int i = 0, j = 1; i < inputStrings.length && j < inputStrings.length; i++, j++) {
			byte[] iByteArray = ByteArrayAndInteger.findFirstIntegerMatch(collationKeysAndIndexes, i).byteArray;
			byte[] jByteArray = ByteArrayAndInteger.findFirstIntegerMatch(collationKeysAndIndexes, j).byteArray;
			boolean isPairEqual = collationKeyComparator.compare(iByteArray, jByteArray) == 0;
			if (!isPairEqual) {
				fail(String.format("Collation keys for inputStrings [%s] and [%s] ([%s], [%s]) were not equal",
						inputStrings[i], inputStrings[j], Hex.encodeHexString(iByteArray),
						Hex.encodeHexString(jByteArray)));
			}
		}
	}

	private void testCollationKeysNotEqual(String[] inputStrings, List<ByteArrayAndInteger> collationKeysAndIndexes)
			throws Exception {
		for (int i = 0; i < inputStrings.length; i++) {
			for (int j = i + 1; j < inputStrings.length; j++) {
				byte[] iByteArray = ByteArrayAndInteger.findFirstIntegerMatch(collationKeysAndIndexes, i).byteArray;
				byte[] jByteArray = ByteArrayAndInteger.findFirstIntegerMatch(collationKeysAndIndexes, j).byteArray;
				boolean isPairEqual = collationKeyComparator.compare(iByteArray, jByteArray) == 0;
				if (isPairEqual) {
					fail(String.format("Collation keys for inputStrings [%s] and [%s] ([%s], [%s]) were equal",
							inputStrings[i], inputStrings[j], Hex.encodeHexString(iByteArray),
							Hex.encodeHexString(jByteArray)));
				}
			}
		}
	}

	private static byte[] callFunction(String inputStr, String localeIsoCode, Boolean upperCaseCollator,
			Integer strength, Integer decomposition, SortOrder sortOrder) throws Exception {
		LiteralExpression inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral;
		inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);
		localeIsoCodeLiteral = LiteralExpression.newConstant(localeIsoCode, PVarchar.INSTANCE, sortOrder);
		upperCaseBooleanLiteral = LiteralExpression.newConstant(upperCaseCollator, PBoolean.INSTANCE, sortOrder);
		strengthLiteral = LiteralExpression.newConstant(strength, PInteger.INSTANCE, sortOrder);
		decompositionLiteral = LiteralExpression.newConstant(decomposition, PInteger.INSTANCE, sortOrder);
		return callFunction(inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral);

	}

	private static byte[] callFunction(LiteralExpression inputStrLiteral, LiteralExpression localeIsoCodeLiteral,
			LiteralExpression upperCaseBooleanLiteral, LiteralExpression strengthLiteral,
			LiteralExpression decompositionLiteral) throws Exception {
		List<Expression> expressions = Lists.newArrayList((Expression) inputStrLiteral,
				(Expression) localeIsoCodeLiteral, (Expression) upperCaseBooleanLiteral, (Expression) strengthLiteral,
				(Expression) decompositionLiteral);
		Expression collationKeyFunction = new CollationKeyFunction(expressions);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		boolean ret = collationKeyFunction.evaluate(null, ptr);
		byte[] result = ret
				? (byte[]) collationKeyFunction.getDataType().toObject(ptr, collationKeyFunction.getSortOrder()) : null;
		return result;
	}
}
