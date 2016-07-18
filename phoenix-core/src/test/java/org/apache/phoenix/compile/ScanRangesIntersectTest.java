/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ScanRangesIntersectTest {

    @Test
    public void testPointLookupIntersect() throws Exception {
        List<KeyRange> keys = points("a","j","m","z");
        ScanRanges ranges = ScanRanges.createPointLookup(keys);
        assertIntersect(ranges, "b", "l", "j");
        
    }
    
    private static void assertIntersect(ScanRanges ranges, String lowerRange, String upperRange, String... expectedPoints) {
        List<KeyRange> expectedKeys = points(expectedPoints);
        Collections.sort(expectedKeys,KeyRange.COMPARATOR);
        Scan scan = new Scan();
        scan.setFilter(ranges.getSkipScanFilter());
        byte[] startKey = lowerRange == null ? KeyRange.UNBOUND : PVarchar.INSTANCE.toBytes(lowerRange);
        byte[] stopKey = upperRange == null ? KeyRange.UNBOUND : PVarchar.INSTANCE.toBytes(upperRange);
        Scan newScan = ranges.intersectScan(scan, startKey, stopKey, 0, true);
        if (expectedPoints.length == 0) {
            assertNull(newScan);
        } else {
            assertNotNull(newScan);
            SkipScanFilter filter = (SkipScanFilter)newScan.getFilter();
            assertEquals(expectedKeys, filter.getSlots().get(0));
        }
    }
    
    private static List<KeyRange> points(String... points) {
        List<KeyRange> keys = Lists.newArrayListWithExpectedSize(points.length);
        for (String point : points) {
            keys.add(KeyRange.getKeyRange(PVarchar.INSTANCE.toBytes(point)));
        }
        return keys;
    }
}
