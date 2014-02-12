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

import org.apache.hadoop.hbase.util.Pair;

/**
 * 
 * 
 * @since 1.2.1
 */
public class BigDecimalUtil {

    /**
     * Calculates the precision and scale for BigDecimal arithmetic operation results. It uses the algorithm mentioned
     * <a href="http://db.apache.org/derby/docs/10.0/manuals/reference/sqlj124.html#HDRSII-SQLJ-36146">here</a>
     * @param lp precision of the left operand
     * @param ls scale of the left operand
     * @param rp precision of the right operand
     * @param rs scale of the right operand
     * @param op The operation type
     * @return {@link Pair} comprising of the precision and scale.
     */
    public static Pair<Integer, Integer> getResultPrecisionScale(int lp, int ls, int rp, int rs, Operation op) {
        int resultPrec = 0, resultScale = 0;
        switch (op) {
        case MULTIPLY:
            resultPrec = lp + rp;
            resultScale = ls + rs;
            break;
        case DIVIDE:
            resultPrec = lp - ls + rp + Math.max(ls + rp - rs + 1, 4);
            resultScale = 31 - lp + ls - rs;
            break;
        case ADD:
            resultPrec = 2 * (lp - ls) + ls; // Is this correct? The page says addition -> 2 * (p - s) + s.
            resultScale = Math.max(ls, rs);
            break;
        case AVG:
            resultPrec = Math.max(lp - ls, rp - rs) + 1 + Math.max(ls, rs);
            resultScale = Math.max(Math.max(ls, rs), 4);
            break;
        case OTHERS:
            resultPrec = Math.max(lp - ls, rp - rs) + 1 + Math.max(ls, rs);
            resultScale = Math.max(ls, rs);
        }
        return new Pair<Integer, Integer>(resultPrec, resultScale);
    }
    
    public static enum Operation {
        MULTIPLY, DIVIDE, ADD, AVG, OTHERS;
    }
}