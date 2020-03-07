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
package org.apache.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class InListExpressionTest {

    @Test
    public void testHashCode() throws Exception {
        int valuesNumber = 500000;
        List<ImmutableBytesPtr> values = new ArrayList<>(valuesNumber);
        for (int i = 0; i < valuesNumber; i++) {
            values.add(new ImmutableBytesPtr(Bytes.toBytes(i)));
        }
        InListExpression exp = new InListExpression(values);

        // first time
        long startTs = System.currentTimeMillis();
        int firstHashCode = exp.hashCode();
        long firstTimeCost = System.currentTimeMillis() - startTs;

        // the rest access
        int restAccessNumber = 3;
        startTs = System.currentTimeMillis();
        List<Integer> hashCodes = Lists.newArrayListWithExpectedSize(restAccessNumber);
        for (int i = 0; i < restAccessNumber; i++) {
            hashCodes.add(exp.hashCode());
        }

        // check time cost
        long restTimeCost = System.currentTimeMillis() - startTs;
        assertTrue("first time: " + firstTimeCost + " <= rest time: " + restTimeCost,
                firstTimeCost > restTimeCost);

        // check hash code
        for (int hashCode : hashCodes) {
            assertEquals("hash code not equal, firstHashCode: " + firstHashCode + ", restHashCode: "
                    + hashCode, firstHashCode, hashCode);
        }
    }
}
