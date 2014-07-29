/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.SequenceInfo;

import com.google.common.math.LongMath;

/**
 * Sequence related util functions
 */
public class SequenceUtil {

    /**
     * Returns the nextValue of a sequence 
     * @throws SQLException if cycle is false and the sequence limit has been reached
     */
    public static boolean checkIfLimitReached(long currentValue, long minValue, long maxValue,
            long incrementBy, long cacheSize) throws SQLException {
        long nextValue = 0;
        boolean increasingSeq = incrementBy > 0 ? true : false;
        // advance currentValue while checking for overflow    
        try {
            long incrementValue = LongMath.checkedMultiply(incrementBy, cacheSize);
            nextValue = LongMath.checkedAdd(currentValue, incrementValue);
        } catch (ArithmeticException e) {
            return true;
        }

        // check if limit was reached
		if ((increasingSeq && nextValue > maxValue)
				|| (!increasingSeq && nextValue < minValue)) {
            return true;
        }
        return false;
    }
    
    public static boolean checkIfLimitReached(SequenceInfo info) throws SQLException {
        return checkIfLimitReached(info.sequenceValue, info.minValue, info.maxValue, info.incrementBy, info.cacheSize);
    }
    
    /**
     * Helper function that returns a {@link SQLException}
     */
    public static SQLException getException(String schemaName, String tableName,
            SQLExceptionCode code) {
        return new SQLExceptionInfo.Builder(code).setSchemaName(schemaName).setTableName(tableName)
                .build().buildException();
    }

}
