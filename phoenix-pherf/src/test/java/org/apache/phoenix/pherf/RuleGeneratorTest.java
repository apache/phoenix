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

package org.apache.phoenix.pherf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.workload.WriteWorkload;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Ignore;
import org.junit.Test;

public class RuleGeneratorTest {
    private static final String matcherScenario = PherfConstants.TEST_SCENARIO_ROOT_PATTERN + ".xml";

    @Test
    public void testDateGenerator() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        DataModel model = parser.getDataModels().get(0);
        WriteWorkload loader = new WriteWorkload(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();

        for (Column dataMapping : model.getDataMappingColumns()) {
            if ((dataMapping.getType() == DataTypeMapping.DATE) && (dataMapping.getName().equals("CREATED_DATE"))) {
                // Test directly through generator method and that it converts to Phoenix type
                assertRandomDateValue(dataMapping, rulesApplier);

                // Test through data value method, which is normal path
                // Do this 20 times and we should hit each possibility at least once.
                for (int i = 0; i < 20; i++) {
                    DataValue value = rulesApplier.getDataValue(dataMapping);
                    assertNotNull("Could not retrieve DataValue for random DATE.", value);
                    assertNotNull("Could not retrieve a value in DataValue for random DATE.", value.getValue());
                    if (value.getMinValue() != null) {
                        // Check that dates are between min/max
                        assertDateBetween(value);
                    }
                }
            }
        }
    }
    
    //Test to check the current date is generated correctly between the timestamps at column level and datavalue level
    @Test
    public void testCurrentDateGenerator() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        DataModel model = parser.getDataModels().get(0);
        WriteWorkload loader = new WriteWorkload(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();

        // Time before generating the date
        String timeStamp1 = rulesApplier.getCurrentDate();
        sleep(2); //sleep for few mili-sec

        for (Column dataMapping : model.getDataMappingColumns()) {
            if ((dataMapping.getType() == DataTypeMapping.DATE)
                    && (dataMapping.getUseCurrentDate() == true)) {

                // Generate the date using rules
                DataValue value = rulesApplier.getDataValue(dataMapping);
                assertNotNull("Could not retrieve DataValue for random DATE.", value);
                assertNotNull("Could not retrieve a value in DataValue for random DATE.",
                        value.getValue());

                sleep(2);
                // Time after generating the date
                String timeStamp2 = rulesApplier.getCurrentDate();

                // Check that dates are between timestamp1 & timestamp2
                value.setMinValue(timeStamp1);
                value.setMaxValue(timeStamp2);
                assertDateBetween(value);
            }

            // Check at list level
            if ((dataMapping.getType() == DataTypeMapping.DATE)
                    && (dataMapping.getName().equals("PRESENT_DATE"))) {
                // Do this 20 times and we should and every time generated data should be between
                // timestamps
                for (int i = 0; i < 1; i++) {
                    DataValue value = rulesApplier.getDataValue(dataMapping);
                    assertNotNull("Could not retrieve DataValue for random DATE.", value);
                    assertNotNull("Could not retrieve a value in DataValue for random DATE.",
                            value.getValue());

                    sleep(2);
                    // Time after generating the date
                    String timeStamp2 = rulesApplier.getCurrentDate();

                    // Check generated date is between timestamp1 & timestamp2
                    value.setMinValue(timeStamp1);
                    value.setMaxValue(timeStamp2);
                    assertDateBetween(value);

                }
            }
        }

    }

    @Test
    public void testNullChance() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        DataModel model = parser.getDataModels().get(0);
        WriteWorkload loader = new WriteWorkload(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();
        int sampleSize = 100;
        List<String> values = new ArrayList<>(sampleSize);

        for (Column dataMapping : model.getDataMappingColumns()) {
            DataValue value = rulesApplier.getDataValue(dataMapping);
            if (dataMapping.getNullChance() == 0) {
                // 0 chance of getting null means we should never have an empty string returned
                assertFalse("", value.getValue().equals(""));
            } else if (dataMapping.getNullChance() == 100) {
                // 100 chance of getting null means we should always have an empty string returned
                assertTrue("", value.getValue().equals(""));
            } else if ((dataMapping.getNullChance() == 90)) {
                // You can't really test for this, but you can eyeball it on debugging.
                for (int i = 0; i < sampleSize; i++) {
                    DataValue tVal = rulesApplier.getDataValue(dataMapping);
                    values.add(tVal.getValue());
                }
                Collections.sort(values);
            }
        }
    }

    @Test
    public void testSequentialDataSequence() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        DataModel model = parser.getDataModels().get(0);
        WriteWorkload loader = new WriteWorkload(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();

        Column targetColumn = null;
        for (Column column : model.getDataMappingColumns()) {
            DataSequence sequence = column.getDataSequence();
            if (sequence == DataSequence.SEQUENTIAL) {
                targetColumn = column;
                break;
            }
        }
        assertNotNull("Could not find a DataSequence.SEQENTIAL rule.", targetColumn);
        assertMultiThreadedIncrementValue(targetColumn, rulesApplier);
    }

    /**
     * Verifies that we can generate a date between to specific dates.
     *
     * @param dataMapping
     * @param rulesApplier
     * @throws Exception
     */
    private void assertRandomDateValue(Column dataMapping, RulesApplier rulesApplier) throws Exception {
        List<DataValue> dataValues = dataMapping.getDataValues();
        DataValue ruleValue = dataValues.get(2);
        String dt = rulesApplier.generateRandomDate(ruleValue.getMinValue(), ruleValue.getMaxValue());
        ruleValue.setValue(dt);
        assertDateBetween(ruleValue);
    }

    /**
     * This method will test {@link org.apache.phoenix.pherf.configuration.DataSequence} SEQUENTIAL
     * It ensures values returned always increase uniquely. RulesApplier will be accessed by multiple writer
     * so we must ensure increment is thread safe.
     */
    private void assertMultiThreadedIncrementValue(final Column column, final RulesApplier rulesApplier) throws Exception {
        final int threadCount = 30;
        final int increments = 100;
        final Set testSet = new TreeSet();
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread() {

                @Override
                public void run() {
                    for (int i = 0; i < increments; i++) {
                        try {
                            DataValue value = rulesApplier.getDataValue(column);
                            String strValue = value.getValue();
                            synchronized (testSet) {
                                assertFalse("Incrementer gave a duplicate value: " + strValue, testSet.contains(strValue));
                                assertTrue("Length did not equal expected.",
                                        strValue.length() == column.getLength());
                                testSet.add(strValue);
                            }
                        } catch (Exception e) {
                            fail("Caught an exception during test: " + e.getMessage());
                        }
                    }
                }
            };
            t.start();
            threadList.add(t);
        }

        // Wait for threads to finish
        for (Thread t : threadList) {
            try {
                t.join();
            } catch (InterruptedException e) {
                fail("There was a problem reading thread: " + e.getMessage());
            }
        }

        assertTrue("Expected count in increments did not match expected",
                testSet.size() == (threadCount * increments));
    }

    @Test
    public void testValueListRule() throws Exception {
        List<String> expectedValues = new ArrayList();
        expectedValues.add("aAAyYhnNbBs9kWk");
        expectedValues.add("bBByYhnNbBs9kWu");
        expectedValues.add("cCCyYhnNbBs9kWr");

        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        WriteWorkload loader = new WriteWorkload(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();
        Scenario scenario = parser.getScenarios().get(0);

        Column simPhxCol = new Column();
        simPhxCol.setName("PARENT_ID");
        simPhxCol.setType(DataTypeMapping.CHAR);

        // Run this 10 times gives a reasonable chance that all the values will appear at least once
        for (int i = 0; i < 10; i++) {
            DataValue value = rulesApplier.getDataForRule(scenario, simPhxCol);
            assertTrue("Got a value not in the list for the rule. :" + value.getValue(), expectedValues.contains(value.getValue()));
        }
    }

    @Test
    public void testRuleOverrides() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        WriteWorkload loader = new WriteWorkload(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();
        Scenario scenario = parser.getScenarios().get(0);

        // We should be able to find the correct rule based only on Type and Name combination
        // Test CHAR
        Column simPhxCol = new Column();
        simPhxCol.setName("OTHER_ID");
        simPhxCol.setType(DataTypeMapping.CHAR);

        // Get the rule we expect to match
        Column rule = rulesApplier.getRule(simPhxCol);
        assertEquals("Did not find the correct rule.", rule.getName(), simPhxCol.getName());
        assertEquals("Did not find the matching rule type.", rule.getType(), simPhxCol.getType());
        assertEquals("Rule contains incorrect length.", rule.getLength(), 8);
        assertEquals("Rule contains incorrect prefix.", rule.getPrefix(), "z0Oxx00");

        DataValue value = rulesApplier.getDataForRule(scenario, simPhxCol);
        assertEquals("Value returned does not match rule.", value.getValue().length(), 8);

        // Test VARCHAR with RANDOM and prefix
        simPhxCol.setName("OLDVAL_STRING");
        simPhxCol.setType(DataTypeMapping.VARCHAR);

        // Get the rule we expect to match
        rule = rulesApplier.getRule(simPhxCol);
        assertEquals("Did not find the correct rule.", rule.getName(), simPhxCol.getName());
        assertEquals("Did not find the matching rule type.", rule.getType(), simPhxCol.getType());
        assertEquals("Rule contains incorrect length.", rule.getLength(), 10);
        assertEquals("Rule contains incorrect prefix.", rule.getPrefix(), "MYPRFX");

        value = rulesApplier.getDataForRule(scenario, simPhxCol);
        assertEquals("Value returned does not match rule.", value.getValue().length(), 10);
        assertTrue("Value returned start with prefix.",
                StringUtils.startsWith(value.getValue(), rule.getPrefix()));
    }

    /**
     * Asserts that the value field is between the min/max value fields
     *
     * @param value
     */
    private void assertDateBetween(DataValue value) {
        DateTimeFormatter fmtr = DateTimeFormat.forPattern(PherfConstants.DEFAULT_DATE_PATTERN).withZone(DateTimeZone.UTC);

        DateTime dt = fmtr.parseDateTime(value.getValue());
        DateTime min = fmtr.parseDateTime(value.getMinValue());
        DateTime max = fmtr.parseDateTime(value.getMaxValue());

        assertTrue("Value " + dt + " is not after minValue", dt.isAfter(min));
        assertTrue("Value " + dt + " is not before maxValue", dt.isBefore(max));
    }

    private void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
