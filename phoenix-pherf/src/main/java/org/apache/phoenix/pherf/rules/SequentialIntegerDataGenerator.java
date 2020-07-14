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

import java.util.concurrent.atomic.AtomicLong;

public class SequentialIntegerDataGenerator implements RuleBasedDataGenerator {
    private final Column columnRule;
    private final AtomicLong counter;
    private final long minValue;
    private final long maxValue;

    public SequentialIntegerDataGenerator(Column columnRule) {
        Preconditions.checkArgument(columnRule.getDataSequence() == DataSequence.SEQUENTIAL);
        Preconditions.checkArgument(isIntegerType(columnRule.getType()));
        this.columnRule = columnRule;
        minValue = columnRule.getMinValue();
        maxValue = columnRule.getMaxValue();
        counter = new AtomicLong(0);
    }

    /**
     * Note that this method rolls over for attempts to get larger than maxValue
     * @return new DataValue
     */
    @Override
    public DataValue getDataValue() {
        return new DataValue(columnRule.getType(), String.valueOf((counter.getAndIncrement() % (maxValue - minValue + 1)) + minValue));
    }

    // Probably could go into a util class in the future
    boolean isIntegerType(DataTypeMapping mapping) {
        switch (mapping) {
        case BIGINT:
        case INTEGER:
        case TINYINT:
        case UNSIGNED_LONG:
            return true;
        default:
            return false;
        }
    }
}
