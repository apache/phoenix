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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.stats.StatisticsScanner.StatisticsScannerCallable;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to verify that we don't try to update stats when a RS is stopping.
 */
public class StatisticsScannerTest {

    private Region region;
    private RegionServerServices rsServices;
    private StatisticsWriter statsWriter;
    private StatisticsScannerCallable callable;
    private StatisticsCollectionRunTracker runTracker;
    private StatisticsScanner mockScanner;
    private StatisticsCollector tracker;
    private InternalScanner delegate;
    private HRegionInfo regionInfo;

    private Configuration config;

    @Before
    public void setupMocks() throws Exception {
        this.config = new Configuration(false);

        // Create all of the mocks
        this.region = mock(Region.class);
        this.rsServices = mock(RegionServerServices.class);
        this.statsWriter = mock(StatisticsWriter.class);
        this.callable = mock(StatisticsScannerCallable.class);
        this.runTracker = mock(StatisticsCollectionRunTracker.class);
        this.mockScanner = mock(StatisticsScanner.class);
        this.tracker = mock(StatisticsCollector.class);
        this.delegate = mock(InternalScanner.class);
        this.regionInfo = mock(HRegionInfo.class);

        // Wire up the mocks to the mock StatisticsScanner
        when(mockScanner.getStatisticsWriter()).thenReturn(statsWriter);
        when(mockScanner.getRegionServerServices()).thenReturn(rsServices);
        when(mockScanner.createCallable()).thenReturn(callable);
        when(mockScanner.getStatsCollectionRunTracker(any(Configuration.class))).thenReturn(runTracker);
        when(mockScanner.getRegion()).thenReturn(region);
        when(mockScanner.getConfig()).thenReturn(config);
        when(mockScanner.getTracker()).thenReturn(tracker);
        when(mockScanner.getDelegate()).thenReturn(delegate);

        // Wire up the HRegionInfo mock to the Region mock
        when(region.getRegionInfo()).thenReturn(regionInfo);

        // Always call close() on the mock StatisticsScanner
        doCallRealMethod().when(mockScanner).close();
    }

    @Test
    public void testCheckRegionServerStoppingOnClose() throws Exception {
        when(rsServices.isStopping()).thenReturn(true);
        when(rsServices.isStopped()).thenReturn(false);

        mockScanner.close();

        verify(rsServices).isStopping();
        verify(callable, never()).call();
        verify(runTracker, never()).runTask(callable);
    }

    @Test
    public void testCheckRegionServerStoppedOnClose() throws Exception {
        when(rsServices.isStopping()).thenReturn(false);
        when(rsServices.isStopped()).thenReturn(true);

        mockScanner.close();

        verify(rsServices).isStopping();
        verify(rsServices).isStopped();
        verify(callable, never()).call();
        verify(runTracker, never()).runTask(callable);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCheckRegionServerStoppingOnException() throws Exception {
        StatisticsScannerCallable realCallable = mockScanner.new StatisticsScannerCallable();
        doThrow(new IOException()).when(statsWriter).deleteStats(any(Region.class), any(StatisticsCollector.class),
                any(ImmutableBytesPtr.class), any(List.class));
        when(rsServices.isStopping()).thenReturn(true);
        when(rsServices.isStopped()).thenReturn(false);

        // Should not throw an exception
        realCallable.call();

        verify(rsServices).isStopping();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCheckRegionServerStoppedOnException() throws Exception {
        StatisticsScannerCallable realCallable = mockScanner.new StatisticsScannerCallable();
        doThrow(new IOException()).when(statsWriter).deleteStats(any(Region.class), any(StatisticsCollector.class),
                any(ImmutableBytesPtr.class), any(List.class));
        when(rsServices.isStopping()).thenReturn(false);
        when(rsServices.isStopped()).thenReturn(true);

        // Should not throw an exception
        realCallable.call();

        verify(rsServices).isStopping();
        verify(rsServices).isStopped();
    }
}
