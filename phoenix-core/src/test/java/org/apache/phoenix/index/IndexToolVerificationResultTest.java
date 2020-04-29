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

package org.apache.phoenix.index;

import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexToolVerificationResultTest {
    private long scanMaxTs = 0L;
    @Test
    public void testVerificationResultFailsWithMissing(){
        IndexToolVerificationResult result = new IndexToolVerificationResult(scanMaxTs);
        IndexToolVerificationResult.PhaseResult afterResult =
            new IndexToolVerificationResult.PhaseResult();
        afterResult.setValidIndexRowCount(1L);
        afterResult.setMissingIndexRowCount(1L);
        result.setAfter(afterResult);
        assertTrue(result.isVerificationFailed());
    }

    @Test
    public void testVerificationResultFailsWithInvalid(){
        IndexToolVerificationResult result = new IndexToolVerificationResult(scanMaxTs);
        IndexToolVerificationResult.PhaseResult afterResult =
            new IndexToolVerificationResult.PhaseResult();
        afterResult.setValidIndexRowCount(1L);
        afterResult.setInvalidIndexRowCount(1L);
        result.setAfter(afterResult);
        assertTrue(result.isVerificationFailed());
    }

    @Test
    public void testVerificationResultPassesWithJustBeyondMaxLookback(){
        IndexToolVerificationResult result = new IndexToolVerificationResult(scanMaxTs);
        IndexToolVerificationResult.PhaseResult afterResult =
            new IndexToolVerificationResult.PhaseResult();
        afterResult.setValidIndexRowCount(1L);
        afterResult.setBeyondMaxLookBackInvalidIndexRowCount(1L);
        afterResult.setBeyondMaxLookBackMissingIndexRowCount(1L);
        result.setAfter(afterResult);
        assertFalse(result.isVerificationFailed());
    }

    @Test
    public void testVerificationResultDoesntFailWithBeforeErrors(){
        IndexToolVerificationResult result = new IndexToolVerificationResult(scanMaxTs);
        IndexToolVerificationResult.PhaseResult beforeResult =
            new IndexToolVerificationResult.PhaseResult();
        beforeResult.setValidIndexRowCount(1L);
        beforeResult.setInvalidIndexRowCount(1L);
        beforeResult.setMissingIndexRowCount(1L);
        IndexToolVerificationResult.PhaseResult afterResult =
            new IndexToolVerificationResult.PhaseResult();
        afterResult.setValidIndexRowCount(3L);
        result.setBefore(beforeResult);
        result.setAfter(afterResult);
        assertFalse(result.isVerificationFailed());
    }

}
