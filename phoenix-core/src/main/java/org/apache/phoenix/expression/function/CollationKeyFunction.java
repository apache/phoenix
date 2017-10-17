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

import java.io.DataInput;
import java.io.IOException;
import java.sql.SQLException;
import java.text.Collator;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.VarBinaryFormatter;

import com.force.db.i18n.LinguisticSort;
import com.force.i18n.LocaleUtils;

/**
 * A Phoenix Function that calculates a collation key for an input string based
 * on a caller-provided locale and collator strength and decomposition settings.
 * 
 * It uses the open-source grammaticus and i18n packages to obtain the collators
 * it needs.
 * 
 * @author snakhoda-sfdc
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

	public static final String NAME = "COLLATION_KEY";

	private Collator collator;

	public CollationKeyFunction() {
	}

	public CollationKeyFunction(List<Expression> children) throws SQLException {
		super(children);
		initialize();
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		initialize();
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		try {
			String inputValue = getInputString(tuple, ptr);
			byte[] collationKeyByteArray = collator.getCollationKey(inputValue).toByteArray();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Collation key bytes: " + VarBinaryFormatter.INSTANCE.format(collationKeyByteArray));
			}

			ptr.set(collationKeyByteArray);
			return true;
		} catch (ExpressionEvaluationException e) {
			LOG.debug("ExpressionEvaluationException caught: " + e.getMessage());
			return false;
		}
	}

	private void initialize() {
		String localeISOCode = getLiteralValue(1, String.class);
		Boolean useSpecialUpperCaseCollator = getLiteralValue(2, Boolean.class);
		Integer collatorStrength = getLiteralValue(3, Integer.class);
		Integer collatorDecomposition = getLiteralValue(4, Integer.class);

		if (LOG.isDebugEnabled()) {
			StringBuilder logInputsMessage = new StringBuilder();
			logInputsMessage.append("Input (literal) arguments:").append("localeISOCode: " + localeISOCode)
					.append(", useSpecialUpperCaseCollator: " + useSpecialUpperCaseCollator)
					.append(", collatorStrength: " + collatorStrength)
					.append(", collatorDecomposition: " + collatorDecomposition);
			LOG.debug(logInputsMessage);
		}

		Locale locale = LocaleUtils.get().getLocaleByIsoCode(localeISOCode);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Locale: " + locale.toLanguageTag()));
		}

		LinguisticSort linguisticSort = LinguisticSort.get(locale);

		collator = BooleanUtils.isTrue(useSpecialUpperCaseCollator) ? linguisticSort.getUpperCaseCollator(false)
				: linguisticSort.getCollator();

		if (collatorStrength != null) {
			collator.setStrength(collatorStrength);
		}

		if (collatorDecomposition != null) {
			collator.setDecomposition(collatorDecomposition);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Collator: [strength: %d, decomposition: %d], Special-Upper-Case: %s",
					collator.getStrength(), collator.getDecomposition(),
					BooleanUtils.isTrue(useSpecialUpperCaseCollator)));
		}
	}

	@Override
	public PDataType getDataType() {
		return PVarbinary.INSTANCE;
	}

	@Override
	public String getName() {
		return "COLLATION_KEY";
	}

	/**
	 * Get the string for which the collation key needs to be calculated (the
	 * first argument)
	 */
	private String getInputString(Tuple tuple, ImmutableBytesWritable ptr) {
		Expression expression = getChildren().get(0);
		if (!expression.evaluate(tuple, ptr)) {
			throw new ExpressionEvaluationException(expression);
		}
		String inputString = (String) PVarchar.INSTANCE.toObject(ptr, expression.getSortOrder());
		if (LOG.isDebugEnabled()) {
			LOG.debug("inputString: " + inputString);
		}
		return inputString;
	}

	private <T> T getLiteralValue(int childIndex, Class<T> type) {
		Expression expression = getChildren().get(childIndex);
		if (LOG.isDebugEnabled()) {
			LOG.debug("child: " + childIndex + ", expression: " + expression);
		}
		if (!(expression instanceof LiteralExpression)) {
			throw new ExpressionEvaluationException(expression);
		}
		return type.cast(((LiteralExpression) expression).getValue());
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