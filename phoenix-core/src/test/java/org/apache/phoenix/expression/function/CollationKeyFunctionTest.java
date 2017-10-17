package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.List;

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

import com.google.common.collect.Lists;

/**
 * "Unit" tests for CollationKeyFunction
 * 
 * @author snakhoda
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

	private static void test(String inputStr, String localeIsoCode, String expectedCollationKeyBytesHex)
			throws Exception {
		boolean ret1 = testExpression(inputStr, localeIsoCode, SortOrder.ASC, expectedCollationKeyBytesHex);
		boolean ret2 = testExpression(inputStr, localeIsoCode, SortOrder.DESC, expectedCollationKeyBytesHex);
		assertEquals(ret1, ret2);
	}

	private static boolean testExpression(String inputStr, String localeIsoCode, SortOrder sortOrder,
			String expectedCollationKeyBytesHex) throws Exception {
		LiteralExpression inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral;
		inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);
		localeIsoCodeLiteral = LiteralExpression.newConstant(localeIsoCode, PVarchar.INSTANCE, sortOrder);
		upperCaseBooleanLiteral = LiteralExpression.newConstant(Boolean.FALSE, PBoolean.INSTANCE, sortOrder);
		strengthLiteral = LiteralExpression.newConstant(null, PInteger.INSTANCE, sortOrder);
		decompositionLiteral = LiteralExpression.newConstant(null, PInteger.INSTANCE, sortOrder);
		boolean ret = testExpression(inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral, expectedCollationKeyBytesHex);
		return ret;
	}

	private static boolean testExpression(LiteralExpression inputStrLiteral, LiteralExpression localeIsoCodeLiteral,
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
			
			assertEquals(
					String.format("Size of collation key byte arrays was not as expected for [%s], [%s]",
							inputStrLiteral.getValue(), localeIsoCodeLiteral.getValue()),
					expectedCollationKeyByteArray.length, result.length);
			for (int i = 0; i < expectedCollationKeyByteArray.length; i++) {
				assertEquals(
						String.format(
								"Element " + i + " of collation key byte array was not as expected for [%s], [%s]",
								inputStrLiteral.getValue(), localeIsoCodeLiteral.getValue()),
						expectedCollationKeyByteArray[i], result[i]);
			}

		} else {
			fail(String.format("CollationKeyFunction.evaluate returned false for %s, %s", inputStrLiteral.getValue(),
					localeIsoCodeLiteral.getValue()));
		}
		return ret;
	}
}
