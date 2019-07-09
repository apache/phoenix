/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema.stats;

import static org.junit.Assert.assertEquals;

import org.apache.phoenix.util.SizedUtil;

import org.junit.Test;

public class GuidePostEstimationTest {
    @Test
    public void testGetEstimatedSize() {
        GuidePostEstimation estimation = new GuidePostEstimation();
        assertEquals(SizedUtil.LONG_SIZE * 3, estimation.getEstimatedSize());
    }

    @Test
    public void testMerge() {
        GuidePostEstimation estimation = new GuidePostEstimation();
        assertEquals(0, estimation.getByteCount());
        assertEquals(0, estimation.getRowCount());
        assertEquals(GuidePostEstimation.MAX_TIMESTAMP, estimation.getTimestamp());

        long startByteCount = 3;
        long startRowCount = 5;
        long startTimestamp = 10;
        long expectedByteCount = 0;
        long expectedRowCount = 0;
        long expectedTimestamp = GuidePostEstimation.MAX_TIMESTAMP;

        for (int i = 0; i < 10; i++) {
            expectedByteCount += startByteCount;
            expectedRowCount += startRowCount;
            expectedTimestamp = Math.min(expectedTimestamp, startTimestamp);
            GuidePostEstimation temp = new GuidePostEstimation(startByteCount++, startRowCount++, startTimestamp--);
            estimation.merge(temp);
            assertEquals(expectedByteCount, estimation.getByteCount());
            assertEquals(expectedRowCount, estimation.getRowCount());
            assertEquals(expectedTimestamp, estimation.getTimestamp());
        }
    }
}
