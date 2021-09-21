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

/**
 * A generator to round robin thru a list of values.
 */

public class SequentialListDataGenerator implements RuleBasedDataGenerator {
    private final Column columnRule;
    private final AtomicLong counter;

    public SequentialListDataGenerator(Column columnRule) {
        Preconditions.checkArgument(columnRule.getDataSequence() == DataSequence.SEQUENTIAL);
        Preconditions.checkArgument(columnRule.getDataValues().size() > 0);
        Preconditions.checkArgument(isAllowedType(columnRule.getType()));
        this.columnRule = columnRule;
        counter = new AtomicLong(0);
    }

    /**
     * Note that this method rolls over for attempts to get larger than maxValue
     * @return new DataValue
     */
    @Override
    public DataValue getDataValue() {
        long pos = counter.getAndIncrement();
        int index = (int) pos % columnRule.getDataValues().size();
        return columnRule.getDataValues().get(index);
    }

    boolean isAllowedType(DataTypeMapping mapping) {
        // For now only varchar list are supported
        switch (mapping) {
        case VARCHAR:
        case VARBINARY:
        case CHAR:
            return true;
        default:
            return false;
        }
    }
}
