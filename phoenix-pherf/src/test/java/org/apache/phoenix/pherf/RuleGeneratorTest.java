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

import org.apache.phoenix.pherf.configuration.*;
import org.apache.phoenix.pherf.loaddata.DataLoader;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.sql.Types;
import java.util.*;

import static org.junit.Assert.*;

public class RuleGeneratorTest extends BaseTestWithCluster {

    @Test
    public void testDateGenerator() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        DataModel model = parser.getDataModels().get(0);
        DataLoader loader = new DataLoader(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();
        int sampleSize = 100;
        List<String> values = new ArrayList<>(sampleSize);

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

    @Test
    public void testNullChance() throws Exception {
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);
        DataModel model = parser.getDataModels().get(0);
        DataLoader loader = new DataLoader(parser);
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
        DataLoader loader = new DataLoader(parser);
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

        assertTrue("Expected count in increments did not match expected", testSet.size() == (threadCount * increments));
    }

    @Test
    public void testValueListRule() throws Exception {
        List<String> expectedValues = new ArrayList();
        expectedValues.add("aAAyYhnNbBs9kWk");
        expectedValues.add("bBByYhnNbBs9kWu");
        expectedValues.add("cCCyYhnNbBs9kWr");

        XMLConfigParser parser = new XMLConfigParser(".*test_scenario.xml");
        DataLoader loader = new DataLoader(parser);
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

    /**
     * Asserts that the value field is between the min/max value fields
     *
     * @param value
     */
    private void assertDateBetween(DataValue value) {
        DateTimeFormatter fmtr = DateTimeFormat.forPattern(PherfConstants.DEFAULT_DATE_PATTERN);

        DateTime dt = fmtr.parseDateTime(value.getValue());
        DateTime min = fmtr.parseDateTime(value.getMinValue());
        DateTime max = fmtr.parseDateTime(value.getMaxValue());

        assertTrue("Value " + dt + " is not after minValue", dt.isAfter(min));
        assertTrue("Value " + dt + " is not before maxValue", dt.isBefore(max));
    }
}
