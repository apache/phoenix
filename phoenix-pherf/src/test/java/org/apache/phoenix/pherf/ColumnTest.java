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

import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ColumnTest {
    @Test
    public void testColumnMutate() {
        Column columnA = new Column();
        Column columnB = new Column();
        Column columnC = new Column();
        columnA.setType(DataTypeMapping.VARCHAR);
        columnB.setType(DataTypeMapping.VARCHAR);
        columnA.setLength(15);
        columnA.setMinValue(20);
        columnA.setMaxValue(25);
        columnB.setLength(30);
        columnC.setMaxValue(45);

        columnA.mutate(columnB);
        assertTrue("Mutation failed length", columnA.getLength() == columnB.getLength());
        columnA.mutate(columnC);
        assertTrue("Mutation failed length", columnA.getLength() == columnB.getLength());
        assertTrue("Mutation failed min", columnA.getMinValue() == 20);
        assertTrue("Mutation failed max", columnA.getMaxValue() == columnC.getMaxValue());
        assertTrue("Mutation failed name", columnA.getName() == null);

    }
}
