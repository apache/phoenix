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
package org.apache.phoenix.iterate;

import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link OrderedResultIterator}.
 */
public class OrderedResultIteratorTest {

  @Test
  public void testNullIteratorOnClose() throws SQLException {
      ResultIterator delegate =  ResultIterator.EMPTY_ITERATOR;
      List<OrderByExpression> orderByExpressions = Collections.singletonList(null);
      int thresholdBytes = Integer.MAX_VALUE;
      boolean spoolingEnabled = true;
      OrderedResultIterator iterator =
              new OrderedResultIterator(delegate, orderByExpressions, spoolingEnabled,
                      thresholdBytes);
        // Should not throw an exception
      iterator.close();
    }

    @Test
    public void testSpoolingBackwardCompatibility() {
        RegionScanner s = Mockito.mock(RegionScanner.class);
        RegionInfo regionInfo = Mockito.mock(RegionInfo.class);
        Mockito.when(s.getRegionInfo()).thenReturn(regionInfo);
        Scan scan = new Scan();
        Expression exp = LiteralExpression.newConstant(Boolean.TRUE);
        OrderByExpression ex = OrderByExpression.createByCheckIfOrderByReverse(exp, false, false, false);
        ScanPlan.serializeScanRegionObserverIntoScan(scan, 0, Arrays.asList(ex), 100);
        // Check 5.1.0 & Check > 5.1.0
        ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.1.0"));
        NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);

        ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.2.0"));
        NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
        // Check 4.15.0 Check > 4.15.0
        ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("4.15.0"));
        NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
        ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("4.15.1"));
        NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);

        // Check < 5.1
        ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("5.0.0"));
        try {
            NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
            fail("Deserialize should fail for 5.0.0 since we didn't serialize thresholdBytes");
        } catch (IllegalArgumentException e) {
        }
        // Check < 4.15
        ScanUtil.setClientVersion(scan, VersionUtil.encodeVersion("4.14.0"));
        try {
            NonAggregateRegionScannerFactory.deserializeFromScan(scan, s, false, 100);
            fail("Deserialize should fail for 4.14.0 since we didn't serialize thresholdBytes");
        } catch (IllegalArgumentException e) {
        }

    }
}
