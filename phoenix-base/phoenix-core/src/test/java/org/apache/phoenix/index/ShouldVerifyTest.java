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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.when;

public class ShouldVerifyTest {

    @Mock IndexRebuildRegionScanner scanner;
    @Mock IndexMaintainer im;
    @Mock Scan scan;
    @Mock Region region;
    @Mock IndexVerificationResultRepository resultRepository;
    byte[] indexRowKey;
    @Mock IndexToolVerificationResult verificationResult;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        indexRowKey = null;
        when(im.getIndexTableName()).thenReturn(Bytes.toBytes("indexName"));
        when(scanner.shouldVerify(any(IndexTool.IndexVerifyType.class), Matchers.<byte[]>any(), any(Scan.class),
                any(Region.class), any(IndexMaintainer.class),
                any(IndexVerificationResultRepository.class), anyBoolean())).thenCallRealMethod();
        when(scanner.shouldVerify()).thenCallRealMethod();
    }

    @Test
    public void testShouldVerify_repair_true() throws IOException {
        indexRowKey = new byte[5];
        Assert.assertTrue(scanner.shouldVerify(IndexTool.IndexVerifyType.ONLY, indexRowKey, scan, region, im, resultRepository, false));
    }

    @Test
    public void testShouldVerify_repair_rebuild_true() throws IOException {
        indexRowKey = new byte[5];
        when(scan.getAttribute(UngroupedAggregateRegionObserver.INDEX_RETRY_VERIFY)).thenReturn(Bytes.toBytes(1L));
        assertShouldVerify(true);
    }

    private void assertShouldVerify(boolean assertion) throws IOException {
        Assert.assertEquals(assertion, scanner.shouldVerify(IndexTool.IndexVerifyType.NONE, indexRowKey, scan, region, im, resultRepository, false));
        Assert.assertEquals(assertion, scanner.shouldVerify(IndexTool.IndexVerifyType.BEFORE, indexRowKey, scan, region, im, resultRepository, false));
        Assert.assertEquals(assertion, scanner.shouldVerify(IndexTool.IndexVerifyType.AFTER, indexRowKey, scan, region, im, resultRepository, false));
    }

    @Test
    public void testShouldVerify_false() throws IOException {
        when(scan.getAttribute(UngroupedAggregateRegionObserver.INDEX_RETRY_VERIFY)).thenReturn(Bytes.toBytes(1L));
        when(resultRepository.getVerificationResult(1L, scan, region, im.getIndexTableName())).thenReturn(verificationResult);
        assertShouldVerify(false);
    }

    @Test
    public void testShouldVerify_rebuild_true() throws IOException {
        when(scan.getAttribute(UngroupedAggregateRegionObserver.INDEX_RETRY_VERIFY)).thenReturn(Bytes.toBytes(1L));
        when(resultRepository.getVerificationResult(1L, scan, region, im.getIndexTableName())).thenReturn(null);
        assertShouldVerify(true);
    }

    @Test
    public void testShouldVerify_noTime_true() throws IOException {
        when(resultRepository.getVerificationResult(1L, scan, region, im.getIndexTableName())).thenReturn(verificationResult);
        assertShouldVerify(true);
    }
}
