/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import java.math.BigDecimal;

import org.apache.phoenix.schema.types.PDataType;

/**
 * Utility methods for numbers like decimal, long, etc.
 *
 * 
 * @since 0.1
 */
public class NumberUtil {
    
    public static final String DEFAULT_NUMBER_FORMAT = "#,##0.###";

    /**
     * Strip all trailing zeros to ensure that no digit will be zero and
     * round using our default context to ensure precision doesn't exceed max allowed.
     * @return new {@link BigDecimal} instance
     */
    public static BigDecimal normalize(BigDecimal bigDecimal) {
        return bigDecimal.round(PDataType.DEFAULT_MATH_CONTEXT).stripTrailingZeros();
    }

    public static BigDecimal setDecimalWidthAndScale(BigDecimal decimal, Integer precisionOrNull, Integer scaleOrNull) {
        int precision = precisionOrNull == null ? PDataType.MAX_PRECISION : precisionOrNull;
        int scale = scaleOrNull == null ? 0 : scaleOrNull;
        // If we could not fit all the digits before decimal point into the new desired precision and
        // scale, return null and the caller method should handle the error.
        if (((precision - scale) < (decimal.precision() - decimal.scale()))){
            return null;
        }
        if (scaleOrNull != null) {
            decimal = decimal.setScale(scale, BigDecimal.ROUND_DOWN); // FIXME: should this be ROUND_UP?
        }
        return decimal;
    }
}
