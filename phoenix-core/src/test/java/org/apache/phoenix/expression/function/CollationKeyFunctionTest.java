package org.apache.phoenix.expression.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.CollationKeyFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;

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
		test("\u963f", "zh", new Integer[] { 2, 235, 0, 0, 0, 1, 0, 0 });
		test("\u55c4", "zh", new Integer[] { 20, 173, 0, 0, 0, 1, 0, 0 });
		test("\u963e", "zh", new Integer[] { 128, 0, 150, 63, 0, 0, 0, 1, 0, 1, 0, 0 });
		test("\u554a", "zh", new Integer[] { 2, 234, 0, 0, 0, 1, 0, 0 });
		test("\u4ec8", "zh", new Integer[] { 128, 0, 78, 201, 0, 0, 0, 1, 0, 1, 0, 0 });
		test("\u3d9a", "zh", new Integer[] { 128, 0, 61, 155, 0, 0, 0, 1, 0, 1, 0, 0 });
		test("\u9f51", "zh", new Integer[] { 25, 5, 0, 0, 0, 1, 0, 0 });
		
		// Chinese (Taiwan)
		test("\u963f", "zh_TW", new Integer[] { 6, 61, 0, 0, 0, 1, 0, 0 });
		test("\u55c4", "zh_TW", new Integer[] { 36, 30, 0, 0, 0, 1, 0, 0 });
		test("\u963e", "zh_TW", new Integer[] { 128, 0, 150, 63, 0, 0, 0, 1, 0, 1, 0, 0 });
		test("\u554a", "zh_TW", new Integer[] { 9, 201, 0, 0, 0, 1, 0, 0 });
		test("\u4ec8", "zh_TW", new Integer[] { 24, 27, 0, 0, 0, 1, 0, 0 });
		test("\u3d9a", "zh_TW", new Integer[] { 128, 0, 61, 155, 0, 0, 0, 1, 0, 1, 0, 0 });
		test("\u9f51", "zh_TW", new Integer[] { 128, 0, 159, 82, 0, 0, 0, 1, 0, 1, 0, 0 });
		
		// Chinese (Taiwan, Stroke)
		test("\u963f", "zh_TW_STROKE", new Integer[] { 84, 80, 1, 5, 0 });
		test("\u55c4", "zh_TW_STROKE", new Integer[] { 115, 52, 1, 5, 0 });
		test("\u963e", "zh_TW_STROKE", new Integer[] { 84, 79, 1, 5, 0 });
		test("\u554a", "zh_TW_STROKE", new Integer[] { 98, 222, 1, 5, 0 });
		test("\u4ec8", "zh_TW_STROKE", new Integer[] { 70, 190, 1, 5, 0 });
		test("\u3d9a", "zh_TW_STROKE", new Integer[] { 165, 3, 146, 1, 5, 0 });
		test("\u9f51", "zh_TW_STROKE", new Integer[] { 137, 21, 1, 5, 0 });
		
		// Chinese (China, Stroke)
		test("\u963f", "zh__STROKE", new Integer[] { 40, 1, 5, 0 });
		test("\u55c4", "zh__STROKE", new Integer[] { 42, 1, 5, 0 });
		test("\u963e", "zh__STROKE", new Integer[] { 117, 117, 1, 5, 0 });
		test("\u554a", "zh__STROKE", new Integer[] { 43, 1, 5, 0 });
		test("\u4ec8", "zh__STROKE", new Integer[] { 81, 161, 1, 5, 0 });
		test("\u3d9a", "zh__STROKE", new Integer[] { 165, 3, 146, 1, 5, 0 });
		test("\u9f51", "zh__STROKE", new Integer[] { 105, 53, 1, 5, 0 });
		
		// Chinese (China, Pinyin)
		test("\u963f", "zh__PINYIN", new Integer[] { 40, 1, 5, 0 });
		test("\u55c4", "zh__PINYIN", new Integer[] { 42, 1, 5, 0 });
		test("\u963e", "zh__PINYIN", new Integer[] { 117, 117, 1, 5, 0 });
		test("\u554a", "zh__PINYIN", new Integer[] { 43, 1, 5, 0 });
		test("\u4ec8", "zh__PINYIN", new Integer[] { 81, 161, 1, 5, 0 });
		test("\u3d9a", "zh__PINYIN", new Integer[] { 165, 3, 146, 1, 5, 0 });
		test("\u9f51", "zh__PINYIN", new Integer[] { 105, 53, 1, 5, 0 });
		
	}

	private static void test(String inputStr, String localeIsoCode, Integer[] expectedCollationKeyBytes)
			throws SQLException {
		boolean ret1 = testExpression(inputStr, localeIsoCode, SortOrder.ASC, expectedCollationKeyBytes);
		boolean ret2 = testExpression(inputStr, localeIsoCode, SortOrder.DESC, expectedCollationKeyBytes);
		assertEquals(ret1, ret2);
	}

	private static boolean testExpression(String inputStr, String localeIsoCode, SortOrder sortOrder,
			Integer[] expectedCollationKeyBytes) throws SQLException {
		LiteralExpression inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral;
		inputStrLiteral = LiteralExpression.newConstant(inputStr, PVarchar.INSTANCE, sortOrder);
		localeIsoCodeLiteral = LiteralExpression.newConstant(localeIsoCode, PVarchar.INSTANCE, sortOrder);
		upperCaseBooleanLiteral = LiteralExpression.newConstant(Boolean.FALSE, PBoolean.INSTANCE, sortOrder);
		strengthLiteral = LiteralExpression.newConstant(null, PInteger.INSTANCE, sortOrder);
		decompositionLiteral = LiteralExpression.newConstant(null, PInteger.INSTANCE, sortOrder);
		boolean ret = testExpression(inputStrLiteral, localeIsoCodeLiteral, upperCaseBooleanLiteral, strengthLiteral,
				decompositionLiteral, new PhoenixArray(PInteger.INSTANCE, expectedCollationKeyBytes));
		return ret;
	}

	private static boolean testExpression(LiteralExpression inputStrLiteral, LiteralExpression localeIsoCodeLiteral,
			LiteralExpression upperCaseBooleanLiteral, LiteralExpression strengthLiteral,
			LiteralExpression decompositionLiteral, PhoenixArray expectedCollationKeyByteArray) throws SQLException {
		List<Expression> expressions = Lists.newArrayList((Expression) inputStrLiteral,
				(Expression) localeIsoCodeLiteral, (Expression) upperCaseBooleanLiteral, (Expression) strengthLiteral,
				(Expression) decompositionLiteral);
		Expression collationKeyFunction = new CollationKeyFunction(expressions);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		boolean ret = collationKeyFunction.evaluate(null, ptr);
		if (ret) {
			PhoenixArray result = (PhoenixArray) collationKeyFunction.getDataType().toObject(ptr,
					collationKeyFunction.getSortOrder());

			assertEquals(
					String.format("Size of collation key byte arrays was not as expected for [%s], [%s]",
							inputStrLiteral.getValue(), localeIsoCodeLiteral.getValue()),
					expectedCollationKeyByteArray.getDimensions(), result.getDimensions());
			for (int i = 0; i < expectedCollationKeyByteArray.getDimensions(); i++) {
				assertEquals(
						String.format(
								"Element " + i + " of collation key byte array was not as expected for [%s], [%s]",
								inputStrLiteral.getValue(), localeIsoCodeLiteral.getValue()),
						expectedCollationKeyByteArray.getElement(i), result.getElement(i));
			}

		} else {
			fail(String.format("CollationKeyFunction.evaluate returned false for %s, %s", inputStrLiteral.getValue(),
					localeIsoCodeLiteral.getValue()));
		}
		return ret;
	}
}
