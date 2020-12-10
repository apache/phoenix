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

import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.junit.Test;

import static org.apache.phoenix.pherf.configuration.DataTypeMapping.INTEGER;
import static org.apache.phoenix.pherf.configuration.DataTypeMapping.VARCHAR;
import static org.junit.Assert.assertEquals;

public class SequentialIntegerDataGeneratorTest {
    SequentialIntegerDataGenerator generator;

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonSequential() {
        Column columnA = new Column();
        columnA.setType(INTEGER);
        columnA.setDataSequence(DataSequence.RANDOM);

        //should reject this Column
        generator = new SequentialIntegerDataGenerator(columnA);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonInteger() {
        Column columnA = new Column();
        columnA.setType(VARCHAR);
        columnA.setDataSequence(DataSequence.SEQUENTIAL);

        //should reject this Column
        generator = new SequentialIntegerDataGenerator(columnA);
    }

    @Test
    public void testGetDataValue() {
        Column columnA = new Column();
        columnA.setType(INTEGER);
        columnA.setDataSequence(DataSequence.SEQUENTIAL);
        columnA.setMinValue(1);
        columnA.setMaxValue(3);

        generator = new SequentialIntegerDataGenerator(columnA);
        DataValue result1 = generator.getDataValue();
        assertEquals("1", result1.getValue());
        DataValue result2 = generator.getDataValue();
        assertEquals("2", result2.getValue());
        DataValue result3 = generator.getDataValue();
        assertEquals("3", result3.getValue());
        DataValue result4 = generator.getDataValue();
        assertEquals("1", result4.getValue());
    }
}
