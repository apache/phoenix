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

import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.VarBinaryFormatter;
import org.apache.phoenix.util.i18n.LinguisticSort;
import org.apache.phoenix.util.i18n.LocaleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Phoenix Function that calculates a collation key for an input string based
 * on a caller-provided locale and collator strength and decomposition settings.
 * 
 * The locale should be specified as xx_yy_variant where xx is the ISO 639-1
 * 2-letter language code, yy is the the ISO 3166 2-letter country code. Both
 * countryCode and variant are optional. For example, zh_TW_STROKE, zh_TW and zh
 * are all valid locale representations. Note the language code, country code
 * and variant are used as arguments to the constructor of java.util.Locale.
 *
 * This function originally used the open-source i18n-util package to obtain the
 * collators it needs from the provided locale. As i18n-util is not maintained
 * anymore, the relevant parts from it were copied into Phoenix.
 * See: https://issues.apache.org/jira/browse/PHOENIX-6818
 *
 * The LinguisticSort implementation from i18n-util encapsulates sort-related
 * functionality for a substantive list of locales. For each locale, it provides
 * a collator and an Oracle-specific database function that can be used to sort
 * strings according to the natural language rules of that locale.
 *
 * This function uses the collator returned by LinguisticSort.getCollator to
 * produce a collation key for its input string. A user can expect that the
 * sorting semantics of this function for a given locale is equivalent to the
 * sorting behaviour of an Oracle query that is constructed using the Oracle
 * functions returned by LinguisticSort for that locale.
 *
 * The optional third argument to the function is a boolean that specifies
 * whether to use the upper-case collator (case-insensitive) returned by
 * LinguisticSort.getUpperCaseCollator.
 *
 * The optional fourth and fifth arguments are used to set respectively the
 * strength and composition of the collator returned by LinguisticSort using the
 * setStrength and setDecomposition methods of java.text.Collator.
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

	private static final Logger LOGGER = LoggerFactory.getLogger(CollationKeyFunction.class);

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
		Expression expression = getChildren().get(0);
		if (!expression.evaluate(tuple, ptr)) {
			return false;
		}
		String inputString = (String) PVarchar.INSTANCE.toObject(ptr, expression.getSortOrder());
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("CollationKey inputString: " + inputString);
		}

		if (inputString == null) {
			return true;
		}

		byte[] collationKeyByteArray = collator.getCollationKey(inputString).toByteArray();

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("CollationKey bytes: " +
					VarBinaryFormatter.INSTANCE.format(collationKeyByteArray));
		}

		ptr.set(collationKeyByteArray);
		return true;
	}

	private void initialize() {
		String localeISOCode = getLiteralValue(1, String.class);
		Boolean useSpecialUpperCaseCollator = getLiteralValue(2, Boolean.class);
		Integer collatorStrength = getLiteralValue(3, Integer.class);
		Integer collatorDecomposition = getLiteralValue(4, Integer.class);

		if (LOGGER.isTraceEnabled()) {
			StringBuilder logInputsMessage = new StringBuilder();
			logInputsMessage.append("Input (literal) arguments:").append("localeISOCode: " + localeISOCode)
					.append(", useSpecialUpperCaseCollator: " + useSpecialUpperCaseCollator)
					.append(", collatorStrength: " + collatorStrength)
					.append(", collatorDecomposition: " + collatorDecomposition);
			LOGGER.trace(logInputsMessage.toString());
		}

		Locale locale = LocaleUtils.get().getLocaleByIsoCode(localeISOCode);

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(String.format("Locale: " + locale.toLanguageTag()));
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

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(String.format(
					"Collator: [strength: %d, decomposition: %d], Special-Upper-Case: %s",
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
		return NAME;
	}

	@Override
	public boolean isThreadSafe() {
		// ICU4J Collators are not thread-safe unless they are frozen.
		// TODO: Look into calling freeze() on them to be able return true here.
		return false;
	}
	
    @Override
    public boolean isNullable() {
        return getChildren().get(0).isNullable();
    }
}
