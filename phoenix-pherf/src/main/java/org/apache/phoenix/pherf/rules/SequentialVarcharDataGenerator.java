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
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A generator for sequentially increasing varchar values.
 */
public class SequentialVarcharDataGenerator implements RuleBasedDataGenerator {
    private final Column columnRule;
    private final AtomicLong counter;

    public SequentialVarcharDataGenerator(Column columnRule) {
        Preconditions.checkArgument(columnRule.getDataSequence() == DataSequence.SEQUENTIAL);
        Preconditions.checkArgument(isVarcharType(columnRule.getType()));
        this.columnRule = columnRule;
        counter = new AtomicLong(0);
    }

    /**
     * Add a numerically increasing counter onto the and of a random string.
     * Incremented counter should be thread safe.
     *
     * @return {@link org.apache.phoenix.pherf.rules.DataValue}
     */
    @Override
    public DataValue getDataValue() {
        DataValue data = null;
        long inc = counter.getAndIncrement();
        String strInc = String.valueOf(inc);
        int paddedLength = columnRule.getLengthExcludingPrefix();
        String strInc1 = StringUtils.leftPad(strInc, paddedLength, "x");
        String strInc2 = StringUtils.right(strInc1, columnRule.getLengthExcludingPrefix());
        String varchar = (columnRule.getPrefix() != null) ? columnRule.getPrefix() + strInc2:
                strInc2;

        // Truncate string back down if it exceeds length
        varchar = StringUtils.left(varchar,columnRule.getLength());
        data = new DataValue(columnRule.getType(), varchar);
        return data;
    }

    boolean isVarcharType(DataTypeMapping mapping) {
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
