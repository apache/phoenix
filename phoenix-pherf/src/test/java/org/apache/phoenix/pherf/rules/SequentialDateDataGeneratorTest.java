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

import static org.apache.phoenix.pherf.configuration.DataTypeMapping.DATE;
import static org.apache.phoenix.pherf.configuration.DataTypeMapping.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.junit.Test;

public class SequentialDateDataGeneratorTest {
    SequentialDateDataGenerator generator;

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonSequential() {
        Column columnA = new Column();
        columnA.setType(DATE);
        columnA.setDataSequence(DataSequence.RANDOM);

        //should reject this Column
        generator = new SequentialDateDataGenerator(columnA);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonDate() {
        Column columnA = new Column();
        columnA.setType(VARCHAR);
        columnA.setDataSequence(DataSequence.SEQUENTIAL);

        //should reject this Column
        generator = new SequentialDateDataGenerator(columnA);
    }

    @Test
    public void testGetDataValue() {
        DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        Column columnA = new Column();
        columnA.setType(DATE);
        columnA.setDataSequence(DataSequence.SEQUENTIAL);

        // The increments are the of 1 sec units
        generator = new SequentialDateDataGenerator(columnA);
        LocalDateTime startDateTime = generator.getStartDateTime();

        DataValue result1 = generator.getDataValue();
        LocalDateTime result1LocalTime = LocalDateTime.parse(result1.getValue(), FMT);
        assertFalse(result1LocalTime.isBefore(startDateTime));
        DataValue result2 = generator.getDataValue();
        LocalDateTime result2LocalTime = LocalDateTime.parse(result2.getValue(), FMT);
        assertEquals(result2LocalTime.minusSeconds(1), result1LocalTime);
        DataValue result3 = generator.getDataValue();
        LocalDateTime result3LocalTime = LocalDateTime.parse(result3.getValue(), FMT);
        assertEquals(result3LocalTime.minusSeconds(1), result2LocalTime);
        DataValue result4 = generator.getDataValue();
        LocalDateTime result4LocalTime = LocalDateTime.parse(result4.getValue(), FMT);
        assertEquals(result4LocalTime.minusSeconds(1), result3LocalTime);
    }
}
