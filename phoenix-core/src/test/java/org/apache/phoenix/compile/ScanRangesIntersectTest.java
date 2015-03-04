/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ScanRangesIntersectTest {

    @Test
    public void testPointLookupIntersect() throws Exception {
        RowKeySchema schema = schema();
        int[] slotSpan = ScanUtil.SINGLE_COLUMN_SLOT_SPAN;
        List<KeyRange> keys = points("a","j","m","z");
        ScanRanges ranges = ScanRanges.create(schema, Collections.singletonList(keys), slotSpan);
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
    
    private static RowKeySchema schema() {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(1);
        builder.addField(new PDatum() {
            @Override
            public boolean isNullable() {
                return false;
            }
            @Override
            public PDataType getDataType() {
                return PVarchar.INSTANCE;
            }
            @Override
            public Integer getMaxLength() {
                return null;
            }
            @Override
            public Integer getScale() {
                return null;
            }
            @Override
            public SortOrder getSortOrder() {
                return SortOrder.getDefault();
            }
        }, false, SortOrder.getDefault());
        return builder.build();
    }
}
