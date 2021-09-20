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
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.pherf.configuration.DataTypeMapping.DATE;
import static org.apache.phoenix.pherf.configuration.DataTypeMapping.VARCHAR;
import static org.junit.Assert.assertEquals;

public class SequentialListDataGeneratorTest {
    SequentialListDataGenerator generator;

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonSequential() {
        Column columnA = new Column();
        columnA.setType(VARCHAR);
        columnA.setDataSequence(DataSequence.RANDOM);

        //should reject this Column
        generator = new SequentialListDataGenerator(columnA);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectsNonVarchar() {
        DateTimeFormatter FMT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime startDateTime = new LocalDateTime();
        String formattedDateTime = startDateTime.toString(FMT);
        Column columnA = new Column();
        columnA.setType(DATE);
        columnA.setDataSequence(DataSequence.SEQUENTIAL);
        List<DataValue> values = new ArrayList<>();
        values.add(new DataValue(DATE, formattedDateTime));
        values.add(new DataValue(DATE, formattedDateTime));
        values.add(new DataValue(DATE, formattedDateTime));
        columnA.setDataValues(values);

        //should reject this Column
        generator = new SequentialListDataGenerator(columnA);
    }

    @Test
    public void testGetDataValue() {
        Column columnA = new Column();
        columnA.setType(VARCHAR);
        columnA.setDataSequence(DataSequence.SEQUENTIAL);
        List<DataValue> values = new ArrayList<>();
        values.add(new DataValue(VARCHAR, "A"));
        values.add(new DataValue(VARCHAR, "B"));
        values.add(new DataValue(VARCHAR, "C"));
        columnA.setDataValues(values);

        generator = new SequentialListDataGenerator(columnA);
        DataValue result1 = generator.getDataValue();
        assertEquals("A", result1.getValue());
        DataValue result2 = generator.getDataValue();
        assertEquals("B", result2.getValue());
        DataValue result3 = generator.getDataValue();
        assertEquals("C", result3.getValue());
        DataValue result4 = generator.getDataValue();
        assertEquals("A", result4.getValue());
    }
}
