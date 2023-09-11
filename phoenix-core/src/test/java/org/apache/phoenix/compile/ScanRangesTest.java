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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


/**
 * Test for intersect method in {@link ScanRanges}
 */
@RunWith(Parameterized.class)
public class ScanRangesTest {

    private final ScanRanges scanRanges;
    private final KeyRange keyRange;
    private final boolean expectedResult;

    public ScanRangesTest(ScanRanges scanRanges, int[] widths,
            KeyRange keyRange, boolean expectedResult) {
        this.keyRange = keyRange;
        this.scanRanges = scanRanges;
        this.expectedResult = expectedResult;
    }

    @Test
    public void test() {
        byte[] lowerInclusiveKey = keyRange.getLowerRange();
        if (!keyRange.isLowerInclusive() && !Bytes.equals(lowerInclusiveKey, KeyRange.UNBOUND)) {
            // This assumes the last key is fixed length, otherwise the results may be incorrect
            // since there's no terminating 0 byte for a variable length key and thus we may be
            // incrementing the key too much.
            lowerInclusiveKey = ByteUtil.nextKey(lowerInclusiveKey);
        }
        byte[] upperExclusiveKey = keyRange.getUpperRange();
        if (keyRange.isUpperInclusive()) {
            // This assumes the last key is fixed length, otherwise the results may be incorrect
            // since there's no terminating 0 byte for a variable length key and thus we may be
            // incrementing the key too much.
            upperExclusiveKey = ByteUtil.nextKey(upperExclusiveKey);
        }
        assertEquals(expectedResult, scanRanges.intersectRegion(lowerInclusiveKey,upperExclusiveKey,false));
    }

    @Parameters(name="{0} {2}")
    public static synchronized Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // variable length test that demonstrates that null byte
        // must be added at end
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("b"), false, Bytes.toBytes("c"), true, SortOrder.ASC),
                    }},
                    new int[] {0},
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("ba"), true, Bytes.toBytes("bb"), true, SortOrder.ASC),
                    true));
        // KeyRange covers the first scan range.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a9Z"), true, Bytes.toBytes("c0A"), true, SortOrder.ASC),
                    true));
        // KeyRange that requires a fixed width exclusive lower bound to be bumped up
        // and made inclusive. Otherwise, the comparison thinks its bigger than it really is.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1A"), true, Bytes.toBytes("b1A"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b0A"), true, Bytes.toBytes("b1C"), true, SortOrder.ASC),
                    true));
        // KeyRange intersect with the first scan range on range's upper end.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b0A"), true, Bytes.toBytes("b1B"), true, SortOrder.ASC),
                    true));
         // ScanRanges is everything.
        testCases.addAll(
                foreach(ScanRanges.EVERYTHING, 
                    null,
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                    true));
        // ScanRanges is nothing.
        testCases.addAll(
                foreach(ScanRanges.NOTHING,
                    null,
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                    false));
        // KeyRange below the first scan range.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                    }},
                    new int[] {1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("2"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("C"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b0Y"), true, Bytes.toBytes("b0Z"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("2"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("C"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b0A"), true, Bytes.toBytes("b2A"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1A"), true, Bytes.toBytes("b1B"), false, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("E"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a0Z"), false, Bytes.toBytes("a1A"), false, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("C"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a1A"), true, Bytes.toBytes("b1B"), false, SortOrder.ASC),
                    false));
        // KeyRange intersects with the first scan range on range's lower end.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1C"), true, Bytes.toBytes("b2E"), true, SortOrder.ASC),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1D"), true, Bytes.toBytes("b2E"), true, SortOrder.ASC),
                    true));
        // KeyRange above the first scan range, no intersect.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("H"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1E"), true, Bytes.toBytes("b1F"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("2"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("G"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a1I"), true, Bytes.toBytes("a2A"), false, SortOrder.ASC),
                    false));
        // KeyRange above the first scan range, with intersects.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("I"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1E"), true, Bytes.toBytes("b1H"), true, SortOrder.ASC),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("c"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("d"), true, Bytes.toBytes("d"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("I"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b00"), true, Bytes.toBytes("d00"), true, SortOrder.ASC),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("c"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("d"), true, Bytes.toBytes("d"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("3"), true, Bytes.toBytes("4"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("D"), true, SortOrder.ASC),
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("I"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b20"), true, Bytes.toBytes("b50"), true, SortOrder.ASC),
                    true));
        // KeyRange above the last scan range.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b1B"), false, Bytes.toBytes("b2A"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), false, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), false, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), false, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("b2A"), true, Bytes.toBytes("b2A"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1},
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("c1A"), false, Bytes.toBytes("c9Z"), true, SortOrder.ASC),
                    false));
        // KeyRange contains unbound lower bound.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes("a0Z"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes("a0Z"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("D"), true, Bytes.toBytes("E"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes("a1C"), true, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("D"), true, Bytes.toBytes("E"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes("a1D"), true, SortOrder.ASC),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("D"), true, Bytes.toBytes("E"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes("a2D"), true, SortOrder.ASC),
                    true));
        // KeyRange contains unbound upper bound
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a0A"), true, KeyRange.UNBOUND, false, SortOrder.ASC),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a1B"), true, KeyRange.UNBOUND, false, SortOrder.ASC),
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a1C"), true, KeyRange.UNBOUND, false, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a3A"), true, KeyRange.UNBOUND, false, SortOrder.ASC),
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("d0A"), true, KeyRange.UNBOUND, false, SortOrder.ASC),
                    false));
        // KeyRange is unbound to unbound.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true, SortOrder.ASC),
                        },{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true, SortOrder.ASC),
                    }},
                    new int[] {1,1,1}, 
                    PChar.INSTANCE.getKeyRange(KeyRange.UNBOUND, false, KeyRange.UNBOUND, false, SortOrder.ASC),
                    true));
        return testCases;
    }

    private static Collection<?> foreach(ScanRanges ranges, int[] widths, KeyRange keyRange,
            boolean expectedResult) {
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {ranges, widths, keyRange, expectedResult});
        return ret;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange keyRange,
            boolean expectedResult) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(10);
        for (final int width : widths) {
            if (width > 0) {
                builder.addField(new PDatum() {
                    @Override
                    public boolean isNullable() {
                        return false;
                    }
                    @Override
                    public PDataType getDataType() {
                        return PChar.INSTANCE;
                    }
                    @Override
                    public Integer getMaxLength() {
                        return width;
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
            } else {
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
                        return width;
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
            }
        }
        ScanRanges scanRanges = ScanRanges.createSingleSpan(builder.build(), slots);
        return foreach(scanRanges, widths, keyRange, expectedResult);
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };
}
