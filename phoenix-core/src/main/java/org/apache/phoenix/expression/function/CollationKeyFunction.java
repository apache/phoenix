package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.text.Collator;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PUnsignedIntArray;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PhoenixArray;

import com.force.db.i18n.LinguisticSort;
import com.force.i18n.LocaleUtils;

import com.ibm.icu.impl.jdkadapter.CollatorICU;
import com.ibm.icu.util.ULocale;

/**
 * A Phoenix Function that calculates a collation key for an input string based
 * on a caller-provided locale and collator strength and decomposition settings.
 * 
 * It uses the open-source grammaticus and i18n packages to obtain the collators
 * it needs.
 * 
 * @author snakhoda
 *
 */
@FunctionParseNode.BuiltInFunction(name = CollationKeyFunction.NAME, args = {
		// input string
		@FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
		// ISO Code for Locale
		@FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
		// whether to use special upper case collator
		@FunctionParseNode.Argument(allowedTypes = { PBoolean.class }, defaultValue = "false", isConstant = true),
		// collator strength
		@FunctionParseNode.Argument(allowedTypes = { PInteger.class }, defaultValue = "null", isConstant = true),
		// collator decomposition
		@FunctionParseNode.Argument(allowedTypes = { PInteger.class }, defaultValue = "null", isConstant = true) })
public class CollationKeyFunction extends ScalarFunction {

	private static final Log LOG = LogFactory.getLog(CollationKeyFunction.class);

	public static final String NAME = "COLLKEY";

	public CollationKeyFunction() {
	}

	public CollationKeyFunction(List<Expression> children) throws SQLException {
		super(children);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		try {
			String inputValue = getInputValue(tuple, ptr);
			String localeISOCode = getLocaleISOCode(tuple, ptr);
			Boolean useSpecialUpperCaseCollator = getUseSpecialUpperCaseCollator(tuple, ptr);
			Integer collatorStrength = getCollatorStrength(tuple, ptr);
			Integer collatorDecomposition = getCollatorDecomposition(tuple, ptr);

			Locale locale = LocaleUtils.get().getLocaleByIsoCode(localeISOCode);
			
			if(LOG.isDebugEnabled()) {
				LOG.debug(String.format("Locale: " + locale.toLanguageTag()));
			}
			
			LinguisticSort linguisticSort = LinguisticSort.get(locale);

			Collator collator = BooleanUtils.isTrue(useSpecialUpperCaseCollator)
					? linguisticSort.getUpperCaseCollator(false) : linguisticSort.getCollator();

			if (collatorStrength != null) {
				collator.setStrength(collatorStrength);
			}

			if (collatorDecomposition != null) {
				collator.setDecomposition(collatorDecomposition);
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug(String.format("Collator: [strength: %d, decomposition: %d], Special-Upper-Case: %s",
					collator.getStrength(), collator.getDecomposition(), BooleanUtils.isTrue(useSpecialUpperCaseCollator)));
			}
			
			byte[] collationKeyByteArray = collator.getCollationKey(inputValue).toByteArray();

			if(LOG.isDebugEnabled()) {
				LOG.debug("Collation key bytes:" + Arrays.toString(collationKeyByteArray));
			}
			
			// byte is signed in Java, but we need unsigned values for comparison
			// https://www.programcreek.com/java-api-examples/index.php?api=java.text.CollationKey
			// Byte.toUnsignedInt will convert a byte value between [-128,127] to an int value
			// between [0,255]
			Integer[] collationKeyUnsignedIntArray = new Integer[collationKeyByteArray.length];
			for(int i=0; i < collationKeyByteArray.length; i ++) {
				collationKeyUnsignedIntArray[i] = Byte.toUnsignedInt(collationKeyByteArray[i]);
			}
			
			if(LOG.isDebugEnabled()) {
				LOG.debug("Collation key bytes (unsigned):" + Arrays.toString(collationKeyUnsignedIntArray));
			}
			
			ptr.set(PIntegerArray.INSTANCE.toBytes(new PhoenixArray(PInteger.INSTANCE, collationKeyUnsignedIntArray)));
			return true;
		} catch (ExpressionEvaluationException e) {
			LOG.debug("ExpressionEvaluationException caught: " + e.getMessage());
			return false;
		}
	}

	@Override
	public PDataType getDataType() {
		return PIntegerArray.INSTANCE;
	}

	@Override
	public String getName() {
		return "COLLKEY";
	}

	/**
	 * Get the string for which the collation key needs to be calculated (the
	 * first argument)
	 */
	private String getInputValue(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression inputValueExpression = getEvaluatedExpression(0, tuple, ptr);
		String inputValue = (String) PVarchar.INSTANCE.toObject(ptr, inputValueExpression.getSortOrder());
		if(LOG.isDebugEnabled()) {
			LOG.debug("inputValue: " + inputValue);
		}
		return inputValue;
	}

	/**
	 * Get the ISO Code which represents the locale for which the collator will
	 * be obtained.
	 */
	private String getLocaleISOCode(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression localeISOCodeExpression = getEvaluatedExpression(1, tuple, ptr);
		String localeISOCode = (String) PVarchar.INSTANCE.toObject(ptr, localeISOCodeExpression.getSortOrder());
		if(LOG.isDebugEnabled()) {
			LOG.debug("localeISOCode: " + localeISOCode);
		}
		return localeISOCode;
	}

	/**
	 * Get whether to use LinguisticSort.getUpperCaseCollator (instead of
	 * LinguisticSort.getCollator) to obtain the collator.
	 */
	private Boolean getUseSpecialUpperCaseCollator(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression useSpecialUpperCaseCollatorExpression = getEvaluatedExpression(2, tuple, ptr);
		Boolean useSpecialUpperCaseCollator = (Boolean) PBoolean.INSTANCE.toObject(ptr,
				useSpecialUpperCaseCollatorExpression.getSortOrder());
		if(LOG.isDebugEnabled()) {
			LOG.debug("useSpecialUpperCaseCollator: " + useSpecialUpperCaseCollator);
		}
		return useSpecialUpperCaseCollator;
	}

	/**
	 * Get the collator strength which will be set on the collator before
	 * calculating the collation key.
	 */
	private Integer getCollatorStrength(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression collatorStrengthExpr = getEvaluatedExpression(3, tuple, ptr);
		Integer collatorStrength = (Integer) PInteger.INSTANCE.toObject(ptr, collatorStrengthExpr.getSortOrder());
		if(LOG.isDebugEnabled()) {
			LOG.debug("collatorStrength: " + collatorStrength);
		}
		return collatorStrength;
	}

	/**
	 * Get the collator description which will be set on the collator before
	 * calculating the collation key.
	 */
	private Integer getCollatorDecomposition(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression collatorDecompositionExpr = getEvaluatedExpression(4, tuple, ptr);
		Integer collatorDecomposition = (Integer) PInteger.INSTANCE.toObject(ptr,
				collatorDecompositionExpr.getSortOrder());
		if(LOG.isDebugEnabled()) {
			LOG.debug("collatorDecomposition: " + collatorDecomposition);
		}
		return collatorDecomposition;
	}

	/**
	 * Obtain the evaluated expression at the given argument index.
	 */
	private Expression getEvaluatedExpression(int childIndex, Tuple tuple, ImmutableBytesWritable ptr) {
		Expression expression = getChildren().get(childIndex);
		if(LOG.isDebugEnabled()) {
			LOG.debug("child: " + childIndex + ", expression: " + expression);
		}
		if (!expression.evaluate(tuple, ptr)) {
			throw new ExpressionEvaluationException(expression);
		}
		return expression;
	}

	/**
	 * A private exception class to indicate unsuccessful expression
	 * evaluations.
	 */
	private class ExpressionEvaluationException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		private final Expression expression;

		public ExpressionEvaluationException(Expression expression) {
			this.expression = expression;
		}

		public String getMessage() {
			return "Expression evaluation failed for: " + expression.toString();
		}
	}

}