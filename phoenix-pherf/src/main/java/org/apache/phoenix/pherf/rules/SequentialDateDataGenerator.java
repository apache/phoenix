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
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A generator for sequentially increasing dates.
 * For now the increments are fixed at 1 second.
 */
public class SequentialDateDataGenerator implements RuleBasedDataGenerator {
    private static DateTimeFormatter FMT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final Column columnRule;
    private final AtomicInteger counter;
    private final LocalDateTime startDateTime = new LocalDateTime();

    public SequentialDateDataGenerator(Column columnRule) {
        Preconditions.checkArgument(columnRule.getDataSequence() == DataSequence.SEQUENTIAL);
        Preconditions.checkArgument(isDateType(columnRule.getType()));
        this.columnRule = columnRule;
        counter = new AtomicInteger(0);
    }

    /**
     * Note that this method rolls over for attempts to get larger than maxValue
     * @return new DataValue
     */
    @Override
    public DataValue getDataValue() {
        LocalDateTime newDateTime = startDateTime.plusSeconds(counter.getAndIncrement());
        String formattedDateTime = newDateTime.toString(FMT);
        return new DataValue(columnRule.getType(), formattedDateTime);
    }

    boolean isDateType(DataTypeMapping mapping) {
        switch (mapping) {
        case DATE:
        case TIMESTAMP:
            return true;
        default:
            return false;
        }
    }
}
