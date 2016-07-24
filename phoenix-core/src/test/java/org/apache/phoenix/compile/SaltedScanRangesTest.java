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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;


/**
 * Test for intersect method in {@link ScanRanges} over salted data
 */
@RunWith(Parameterized.class)
public class SaltedScanRangesTest {

    private static Integer nBuckets = 3;
    private final ScanRanges scanRanges;
    private final KeyRange keyRange;
    private final boolean expectedResult;

    public SaltedScanRangesTest(ScanRanges scanRanges, int[] widths,
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
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(KeyRange.UNBOUND, new byte[]{1}),
                    false,
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(new byte[]{1},new byte[]{2}),
                    false,
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(new byte[]{2},KeyRange.UNBOUND),
                    false,
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(new byte[]{1},ByteUtil.concat(new byte[]{1}, Bytes.toBytes("c"))),
                    false,
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(ByteUtil.concat(new byte[]{1}, Bytes.toBytes("e")), new byte[]{2}),
                    false,
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(ByteUtil.concat(new byte[]{1}, Bytes.toBytes("d")), new byte[]{2}),
                    false,
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("h"), true, Bytes.toBytes("i"), false),
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("m"), true, Bytes.toBytes("p"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(ByteUtil.concat(new byte[]{1}, Bytes.toBytes("f")), ByteUtil.concat(new byte[]{1}, Bytes.toBytes("g"))),
                    false,
                    true));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), false),
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("h"), true, Bytes.toBytes("i"), false),
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("m"), true, Bytes.toBytes("p"), false),
                    }},
                    new int[] {0},
                    KeyRange.getKeyRange(ByteUtil.concat(new byte[]{1}, Bytes.toBytes("f")), ByteUtil.concat(new byte[]{1}, Bytes.toBytes("g"))),
                    true,
                    false));
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, KeyRange.UNBOUND, false),
                    }},
                    new int[] {1},
                    KeyRange.getKeyRange(new byte[]{1,0},new byte[]{2,0}),
                    false,
                    true));
        return testCases;
    }

    private static Collection<?> foreach(ScanRanges ranges, int[] widths, KeyRange keyRange,
            boolean expectedResult) {
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {ranges, widths, keyRange, expectedResult});
        return ret;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange keyRange, boolean useSkipScan,
            boolean expectedResult) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        slots = new ArrayList<>(slots);
        slots.add(0, Collections.singletonList(KeyRange.getKeyRange(new byte[]{0})));
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(10);
        builder.addField(SaltingUtil.SALTING_COLUMN, false, SortOrder.getDefault());
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
        ScanRanges scanRanges = ScanRanges.createSingleSpan(builder.build(), slots, nBuckets , useSkipScan);
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
