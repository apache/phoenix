/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.rules;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.pherf.configuration.*;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class RulesApplier {
    private static final Logger LOGGER = LoggerFactory.getLogger(RulesApplier.class);
    private static final AtomicLong COUNTER = new AtomicLong(0);

    // Used to bail out of random distribution if it takes too long
    // This should never happen when distributions add up to 100
    private static final int OH_SHIT_LIMIT = 1000;

    private final Random rndNull;
    private final Random rndVal;
    private final RandomDataGenerator randomDataGenerator;

    private final XMLConfigParser parser;
    private final List<Map> modelList;
    private final Map<String, Column> columnMap;
    private String cachedScenarioOverrideName;
    private Map<DataTypeMapping, List> scenarioOverrideMap;

    private Map<Column,RuleBasedDataGenerator> columnRuleBasedDataGeneratorMap = new HashMap<>();


    public RulesApplier(XMLConfigParser parser) {
        this(parser, EnvironmentEdgeManager.currentTimeMillis());
    }

    public RulesApplier(XMLConfigParser parser, long seed) {
        this.parser = parser;
        this.modelList = new ArrayList<Map>();
        this.columnMap = new HashMap<String, Column>();
        this.rndNull = new Random(seed);
        this.rndVal = new Random(seed);
        this.randomDataGenerator = new RandomDataGenerator();
        this.cachedScenarioOverrideName = null;
        populateModelList();
    }

    public List<Map> getModelList() {
        return Collections.unmodifiableList(this.modelList);
    }
    
    private Map<DataTypeMapping, List> getCachedScenarioOverrides(Scenario scenario) {
    	if (this.cachedScenarioOverrideName == null || this.cachedScenarioOverrideName != scenario.getName()) {
    		this.cachedScenarioOverrideName = scenario.getName();
    		this.scenarioOverrideMap = new HashMap<DataTypeMapping, List>();

    	       if (scenario.getDataOverride() != null) {
				for (Column column : scenario.getDataOverride().getColumn()) {
					List<Column> cols;
					DataTypeMapping type = column.getType();
					if (this.scenarioOverrideMap.containsKey(type)) {
						this.scenarioOverrideMap.get(type).add(column);
					} else {
						cols = new LinkedList<Column>();
						cols.add(column);
						this.scenarioOverrideMap.put(type, cols);
					}
				}
			}
    	}
		return scenarioOverrideMap;
    }


    /**
     * Get a data value based on rules.
     *
     * @param scenario      {@link org.apache.phoenix.pherf.configuration.Scenario} We are getting data for
     * @param phxMetaColumn {@link org.apache.phoenix.pherf.configuration.Column}
     *                      From Phoenix MetaData that are
     *                      generating data for. It defines the
     *                      type we are trying to match.
     * @return
     * @throws Exception
     */
    public DataValue getDataForRule(Scenario scenario, Column phxMetaColumn) throws Exception {
        // TODO Make a Set of Rules that have already been applied so that so we don't generate for every value
    	
        List<Scenario> scenarios = parser.getScenarios();
        DataValue value = null;
        if (scenarios.contains(scenario)) {
            LOGGER.debug("We found a correct Scenario");
            
            Map<DataTypeMapping, List> overrideRuleMap = this.getCachedScenarioOverrides(scenario);
            
            if (overrideRuleMap != null) {
	            List<Column> overrideRuleList = this.getCachedScenarioOverrides(scenario).get(phxMetaColumn.getType());
	            
				if (overrideRuleList != null && overrideRuleList.contains(phxMetaColumn)) {
					LOGGER.debug("We found a correct override column rule");
					Column columnRule = getColumnForRuleOverride(overrideRuleList, phxMetaColumn);
					if (columnRule != null) {
						return getDataValue(columnRule);
					}
				}
            }
            
            // Assume the first rule map
            Map<DataTypeMapping, List> ruleMap = modelList.get(0);
            List<Column> ruleList = ruleMap.get(phxMetaColumn.getType());

            // Make sure Column from Phoenix Metadata matches a rule column
            if (ruleList.contains(phxMetaColumn)) {
                // Generate some random data based on this rule
                LOGGER.debug("We found a correct column rule");
                Column columnRule = getColumnForRule(ruleList, phxMetaColumn);

                value = getDataValue(columnRule);
            } else {
                LOGGER.warn("Attempted to apply rule to data, but could not find a rule to match type:"
                                + phxMetaColumn.getType()
                );
            }

        }
        
        return value;
    }

    /**
     * Get data value based on the supplied rule
     *
     * @param column {@link org.apache.phoenix.pherf.configuration.Column} Column rule to get data for
     * @return {@link org.apache.phoenix.pherf.rules.DataValue} Container Type --> Value mapping
     */
    public DataValue getDataValue(Column column) throws Exception{
        DataValue data = null;
        String prefix = "";
        int length = column.getLength();
        int nullChance = column.getNullChance();
        List<DataValue> dataValues = column.getDataValues();

        // Return an empty value if we fall within the configured probability of null
        if ((nullChance != Integer.MIN_VALUE) && (isValueNull(nullChance))) {
            return new DataValue(column.getType(), "");
        }

        if (column.getPrefix() != null) {
            prefix = column.getPrefix();
        }

        if ((prefix.length() >= length) && (length > 0)) {
            LOGGER.warn("You are attempting to generate data with a prefix (" + prefix + ") "
                    + "That is longer than expected overall field length (" + length + "). "
                    + "This will certainly lead to unexpected data values.");
        }

        switch (column.getType()) {
            case VARCHAR:
            case VARBINARY:
            case CHAR:
                // Use the specified data values from configs if they exist
                if ((column.getDataValues() != null) && (column.getDataValues().size() > 0)) {
                    data = pickDataValueFromList(dataValues);
                } else {
                    Preconditions.checkArgument(length > 0, "length needs to be > 0");
                    if (column.getDataSequence() == DataSequence.SEQUENTIAL) {
                        data = getSequentialVarcharDataValue(column);
                    } else {
                        data = getRandomDataValue(column);
                    }
                }
                break;
            case VARCHAR_ARRAY:
            	//only list datavalues are supported
            	String arr = "";
            	for (DataValue dv : dataValues) {
            		arr += "," + dv.getValue();
            	}
            	if (arr.startsWith(",")) {
            		arr = arr.replaceFirst(",", "");
            	}
            	data = new DataValue(column.getType(), arr);
            	break;
            case DECIMAL:
                if ((column.getDataValues() != null) && (column.getDataValues().size() > 0)) {
                    data = pickDataValueFromList(dataValues);
                } else {
                    int precision = column.getPrecision();
                    double minDbl = column.getMinValue();
                    Preconditions.checkArgument((precision > 0) && (precision <= 18), "Precision must be between 0 and 18");
                    Preconditions.checkArgument(minDbl >= 0, "minvalue must be set in configuration for decimal");
                    Preconditions.checkArgument(column.getMaxValue() > 0, "maxValue must be set in configuration decimal");
                    StringBuilder maxValueStr = new StringBuilder();

                    for (int i = 0; i < precision; i++) {
                        maxValueStr.append(9);
                    }

                    double maxDbl = Math.min(column.getMaxValue(), Double.parseDouble(maxValueStr.toString()));
                    final double dbl = RandomUtils.nextDouble(minDbl, maxDbl);
                    data = new DataValue(column.getType(), String.valueOf(dbl));
                }
                break;
            case TINYINT:
            case INTEGER:
                if ((column.getDataValues() != null) && (column.getDataValues().size() > 0)) {
                    data = pickDataValueFromList(dataValues);
                } else if(DataSequence.SEQUENTIAL.equals(column.getDataSequence())) {
                    RuleBasedDataGenerator generator = getRuleBasedDataGeneratorForColumn(column);
                    data = generator.getDataValue();
                } else {
                    int minInt = (int) column.getMinValue();
                    int maxInt = (int) column.getMaxValue();
                    if (column.getType() == DataTypeMapping.TINYINT) {
                        Preconditions.checkArgument((minInt >= -128) && (minInt <= 128), "min value need to be set in configuration for tinyints " + column.getName());
                        Preconditions.checkArgument((maxInt >= -128) && (maxInt <= 128), "max value need to be set in configuration for tinyints " + column.getName());
                    }
                    int intVal = ThreadLocalRandom.current().nextInt(minInt, maxInt + 1);
                    data = new DataValue(column.getType(), String.valueOf(intVal));
                }
                break;
            case BIGINT:
            case UNSIGNED_LONG:
                if ((column.getDataValues() != null) && (column.getDataValues().size() > 0)) {
                    data = pickDataValueFromList(dataValues);
                } else {
                    long minLong = column.getMinValue();
                    long maxLong = column.getMaxValue();
                    if (column.getType() == DataTypeMapping.UNSIGNED_LONG)
                        Preconditions.checkArgument((minLong > 0) && (maxLong > 0), "min and max values need to be set in configuration for unsigned_longs " + column.getName());
                    long longVal = RandomUtils.nextLong(minLong, maxLong);
                    data = new DataValue(column.getType(), String.valueOf(longVal));
                }
                break;
            case DATE:
            case TIMESTAMP:
                if ((column.getDataValues() != null) && (column.getDataValues().size() > 0)) {
                    data = pickDataValueFromList(dataValues);
                    // Check if date has right format or not
                    data.setValue(checkDatePattern(data.getValue()));
                } else if (column.getUseCurrentDate() != true){
                    int minYear = (int) column.getMinValue();
                    int maxYear = (int) column.getMaxValue();
                    Preconditions.checkArgument((minYear > 0) && (maxYear > 0), "min and max values need to be set in configuration for date/timestamps " + column.getName());

                    String dt = generateRandomDate(minYear, maxYear);
                    data = new DataValue(column.getType(), dt);
                    data.setMaxValue(String.valueOf(minYear));
                    data.setMinValue(String.valueOf(maxYear));
                } else {
                    String dt = getCurrentDate();
                    data = new DataValue(column.getType(), dt);
                }
                break;
            default:
                break;
        }
        Preconditions.checkArgument(data != null,
                "Data value could not be generated for some reason. Please check configs");
        return data;
    }

    // Convert years into standard date format yyyy-MM-dd HH:mm:ss.SSS z
    public String generateRandomDate(int min, int max) throws Exception {
        String mindt = min + "-01-01 00:00:00.000"; // set min date as starting of min year
        String maxdt = max + "-12-31 23:59:59.999"; // set max date as end of max year
        return generateRandomDate(mindt, maxdt);
    }

    public String generateRandomDate(String min, String max) throws Exception {
        DateTimeFormatter fmtr = DateTimeFormat.forPattern(PherfConstants.DEFAULT_DATE_PATTERN).withZone(DateTimeZone.UTC);
        DateTime minDt;
        DateTime maxDt;
        DateTime dt;

        minDt = fmtr.parseDateTime(checkDatePattern(min));
        maxDt = fmtr.parseDateTime(checkDatePattern(max));

        // Get Ms Date between min and max
        synchronized (randomDataGenerator) {
            //Make sure date generated is exactly between the passed limits
            long rndLong = randomDataGenerator.nextLong(minDt.getMillis()+1, maxDt.getMillis()-1);
            dt = new DateTime(rndLong, PherfConstants.DEFAULT_TIME_ZONE);
        }

        return fmtr.print(dt);
    }

    public String getCurrentDate() {
        DateTimeFormatter fmtr = DateTimeFormat.forPattern(PherfConstants.DEFAULT_DATE_PATTERN).withZone(DateTimeZone.UTC);;
        DateTime dt = new DateTime(PherfConstants.DEFAULT_TIME_ZONE);
        return fmtr.print(dt);
    }

    /**
     * Given an int chance [0-100] inclusive, this method will return true if a winner is selected, otherwise false.
     *
     * @param chance Percentage as an int while number.
     * @return boolean if we pick a number within range
     */
    private boolean isValueNull(int chance) {
        return (rndNull.nextInt(100) < chance);
    }

    private DataValue pickDataValueFromList(List<DataValue> values) throws Exception{
        DataValue generatedDataValue = null;
        int sum = 0, count = 0;

        // Verify distributions add up to 100 if they exist
        for (DataValue value : values) {
            int dist = value.getDistribution();
            sum += dist;
        }
        Preconditions.checkArgument((sum == 100) || (sum == 0),
                "Distributions need to add up to 100 or not exist.");

        // Spin the wheel until we get a value.
        while (generatedDataValue == null) {

            // Give an equal chance at picking any one rule to test
            // This prevents rules at the beginning of the list from getting more chances to get picked
            int rndIndex = rndVal.nextInt(values.size());
            DataValue valueRule = values.get(rndIndex);

            generatedDataValue = pickDataValueFromList(valueRule);

            // While it's possible to get here if you have a bunch of really small distributions,
            // It's just really unlikely. This is just a safety just so we actually pick a value.
            if(count++ == OH_SHIT_LIMIT){
                LOGGER.info("We generated a value from hitting our OH_SHIT_LIMIT: " + OH_SHIT_LIMIT);
                generatedDataValue = valueRule;
            }

        }
        return generatedDataValue;
    }

    private DataValue pickDataValueFromList(final DataValue valueRule) throws Exception{
        DataValue retValue = new DataValue(valueRule);

        // Path taken when configuration specifies a specific value to be taken with the <value> tag
        if (valueRule.getValue() != null) {
            int chance = (valueRule.getDistribution() == 0) ? 100 : valueRule.getDistribution();
            return (rndVal.nextInt(100) <= chance) ? retValue : null;
        }

        // Path taken when configuration specifies to use current date
        if (valueRule.getUseCurrentDate() == true) {
            int chance = (valueRule.getDistribution() == 0) ? 100 : valueRule.getDistribution();
            retValue.setValue(getCurrentDate());
            return (rndVal.nextInt(100) <= chance) ? retValue : null;
        }

        // Later we can add support fo other data types if needed.Right now, we just do this for dates
        Preconditions.checkArgument((retValue.getMinValue() != null) || (retValue.getMaxValue() != null), "Both min/maxValue tags must be set if value tag is not used");
        Preconditions.checkArgument((retValue.getType() == DataTypeMapping.DATE), "Currently on DATE is supported for ranged random values");

        retValue.setValue(generateRandomDate(retValue.getMinValue(), retValue.getMaxValue()));

        retValue.setValue(generateRandomDate(retValue.getMinValue(), retValue.getMaxValue()));
        retValue.setMinValue(checkDatePattern(valueRule.getMinValue()));
        retValue.setMaxValue(checkDatePattern(valueRule.getMaxValue()));
        return retValue;
    }

    // Checks if date is in defult pattern
    public String checkDatePattern(String date) {
        DateTimeFormatter fmtr = DateTimeFormat.forPattern(PherfConstants.DEFAULT_DATE_PATTERN).withZone(DateTimeZone.UTC);;
        DateTime parsedDate = fmtr.parseDateTime(date);
        return fmtr.print(parsedDate);
    }

    /**
     * Top level {@link java.util.List} {@link java.util.Map}. This will likely only have one entry until we have
     * multiple files.
     * <p/>
     * <p/>
     * Each Map entry in the List is:
     * {@link java.util.Map} of
     * {@link org.apache.phoenix.pherf.configuration.DataTypeMapping} -->
     * List of {@link org.apache.phoenix.pherf.configuration.Column
     * Build the initial Map with all the general rules.
     * These are contained in:
     * <datamode><datamapping><column>...</column></datamapping></datamode>
     * <p/>
     * <p/>
     * Unsupported until V2
     * Build the overrides by appending them to the list of rules that match the column type
     */
    private void populateModelList() {
        if (!modelList.isEmpty()) {
            return;
        }
        
        // Support for multiple models, but rules are only relevant each model
        for (DataModel model : parser.getDataModels()) {

            // Step 1
            final Map<DataTypeMapping, List> ruleMap = new HashMap<DataTypeMapping, List>();
            for (Column column : model.getDataMappingColumns()) {
            	columnMap.put(column.getName(), column);
            	
                List<Column> cols;
                DataTypeMapping type = column.getType();
                if (ruleMap.containsKey(type)) {
                    ruleMap.get(type).add(column);
                } else {
                    cols = new LinkedList<Column>();
                    cols.add(column);
                    ruleMap.put(type, cols);
                }
            }

            this.modelList.add(ruleMap);
        }
    }

    public Column getRule(Column phxMetaColumn) {
        // Assume the first rule map
        Map<DataTypeMapping, List> ruleMap = modelList.get(0);

        List<Column> ruleList = ruleMap.get(phxMetaColumn.getType());
        return getColumnForRule(ruleList, phxMetaColumn);
    }
    
    public Column getRule(String columnName) {
    	return getRule(columnName, null);
    }
    
    public Column getRule(String columnName, Scenario scenario) {
    	if (null != scenario && null != scenario.getDataOverride()) {
    		for (Column column: scenario.getDataOverride().getColumn()) {
    			if (column.getName().equals(columnName)) {
    				return column;
    			}
    		}
    	}

    	return columnMap.get(columnName);
    }

    private Column getColumnForRuleOverride(List<Column> ruleList, Column phxMetaColumn) {
        for (Column columnRule : ruleList) {
            if (columnRule.getName().equals(phxMetaColumn.getName())) {
                return new Column(columnRule);
            }
        }

       	return null;
    }
    
    private Column getColumnForRule(List<Column> ruleList, Column phxMetaColumn) {

        // Column pointer to head of list
        Column ruleAppliedColumn = new Column(ruleList.get(0));

        // Then we apply each rule override as a mutation to the column
        for (Column columnRule : ruleList) {

            // Check if user defined column rules match the column data type we are generating
            // We don't want to apply the rule if name doesn't match the column from Phoenix
            if (columnRule.isUserDefined()
                    && !columnRule.getName().equals(phxMetaColumn.getName())) {
                continue;
            }
            ruleAppliedColumn.mutate(columnRule);
        }

       	return ruleAppliedColumn;
    }

    /**
     * Add a numerically increasing counter onto the and of a random string.
     * Incremented counter should be thread safe.
     *
     * @param column {@link org.apache.phoenix.pherf.configuration.Column}
     * @return {@link org.apache.phoenix.pherf.rules.DataValue}
     */
    private DataValue getSequentialVarcharDataValue(Column column) {
        DataValue data = null;
        long inc = COUNTER.getAndIncrement();
        String strInc = String.valueOf(inc);
        int paddedLength = column.getLengthExcludingPrefix();
        String strInc1 = StringUtils.leftPad(strInc, paddedLength, "0");
        String strInc2 = StringUtils.right(strInc1, column.getLengthExcludingPrefix());
        String varchar = (column.getPrefix() != null) ? column.getPrefix() + strInc2:
                strInc2;

        // Truncate string back down if it exceeds length
        varchar = StringUtils.left(varchar,column.getLength());
        data = new DataValue(column.getType(), varchar);
        return data;
    }

    private DataValue getRandomDataValue(Column column) {
        String varchar = RandomStringUtils.randomAlphanumeric(column.getLength());
        varchar = (column.getPrefix() != null) ? column.getPrefix() + varchar : varchar;

        // Truncate string back down if it exceeds length
        varchar = StringUtils.left(varchar, column.getLength());
        return new DataValue(column.getType(), varchar);
    }

    private RuleBasedDataGenerator getRuleBasedDataGeneratorForColumn(Column column) {
        RuleBasedDataGenerator generator = columnRuleBasedDataGeneratorMap.get(column);
        if(generator == null) {
            //For now we only have one of these, likely this should replace all all the methods
            generator = new SequentialIntegerDataGenerator(column);
            columnRuleBasedDataGeneratorMap.put(column,generator);
        }
        return generator;
    }
}
