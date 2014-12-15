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
package org.apache.phoenix.util;

import static org.junit.Assert.assertArrayEquals;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;


/**
 * Test the SetKey method in ScanUtil.
 */
@RunWith(Parameterized.class)
public class ScanUtilTest {

    private final List<List<KeyRange>> slots;
    private final byte[] expectedKey;
    private final RowKeySchema schema;
    private final Bound bound;

    public ScanUtilTest(List<List<KeyRange>> slots, int[] widths, byte[] expectedKey, Bound bound) throws Exception {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(widths.length);
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
            }
        }
        this.schema = builder.build();
        this.slots = slots;
        this.expectedKey = expectedKey;
        this.bound = bound;
    }

    @Test
    public void test() {
        byte[] key = new byte[1024];
        int[] position = new int[slots.size()];
        int offset = ScanUtil.setKey(schema, slots, ScanUtil.getDefaultSlotSpans(slots.size()), position, bound, key, 0, 0, slots.size());
        byte[] actualKey = new byte[offset];
        System.arraycopy(key, 0, actualKey, 0, offset);
        assertArrayEquals(expectedKey, actualKey);
    }

    @Parameters(name="{0} {1} {2} {3} {4}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // 1, Lower bound, all single keys, all inclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a1A"),
                Bound.LOWER
                ));
        // 2, Lower bound, all range keys, all inclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a1A"),
                Bound.LOWER
                ));
        // 3, Lower bound, mixed single and range keys, all inclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a1A"),
                Bound.LOWER
                ));
        // 4, Lower bound, all range key, all exclusive on lower bound.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), false, Bytes.toBytes("2"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("b2B"),
                Bound.LOWER
                ));
        // 5, Lower bound, all range key, some exclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), false, Bytes.toBytes("b"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("b1B"),
                Bound.LOWER
                ));
        // 6, Lower bound, mixed single and range key, mixed inclusive and exclusive.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a1B"),
                Bound.LOWER
                ));
        // 7, Lower bound, unbound key in the middle, fixed length.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                        KeyRange.EVERYTHING_RANGE,},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), false, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a"),
                Bound.LOWER
                ));
        // 8, Lower bound, unbound key in the middle, variable length.
        testCases.addAll(
                foreach(new KeyRange[][]{{
                        PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                            KeyRange.EVERYTHING_RANGE,}},
                    new int[] {1,1},
                    PChar.INSTANCE.toBytes("a"),
                    Bound.LOWER
                    ));
        // 9, Lower bound, unbound key at end, variable length.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                        KeyRange.EVERYTHING_RANGE,},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a"),
                Bound.LOWER
                ));
        // 10, Upper bound, all single keys, all inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("a1B"),
                Bound.UPPER
                ));
        // 11, Upper bound, all range keys, all inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("b2C"),
                Bound.UPPER
                ));
        // 12, Upper bound, all range keys, all exclusive, no increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), false),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), false),}},
                new int[] {1,1,1},
                PChar.INSTANCE.toBytes("b2B"),
                Bound.UPPER
                ));
        // 13, Upper bound, single inclusive, range inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), true),}},
                new int[] {1,1},
                PChar.INSTANCE.toBytes("a3"),
                Bound.UPPER
                ));
        // 14, Upper bound, range exclusive, single inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), false),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),}},
                new int[] {1,1},
                PChar.INSTANCE.toBytes("b2"),
                Bound.UPPER
                ));
        // 15, Upper bound, range inclusive, single inclusive, increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("b"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),}},
                new int[] {1,1},
                PChar.INSTANCE.toBytes("b2"),
                Bound.UPPER
                ));
        // 16, Upper bound, single inclusive, range exclusive, no increment at end.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("2"), false),}},
                new int[] {1,1},
                PChar.INSTANCE.toBytes("a2"),
                Bound.UPPER
                ));
        // 17, Upper bound, unbound key, fixed length;
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                        KeyRange.EVERYTHING_RANGE,}},
                new int[] {1,1},
                PChar.INSTANCE.toBytes("b"),
                Bound.UPPER
                ));
        // 18, Upper bound, unbound key, variable length;
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    KeyRange.EVERYTHING_RANGE,}},
                new int[] {1,1},
                PChar.INSTANCE.toBytes("b"),
                Bound.UPPER
                ));
        // 19, Upper bound, keys wrapped around when incrementing.
        testCases.addAll(
            foreach(new KeyRange[][]{{
                PChar.INSTANCE.getKeyRange(new byte[] {-1}, true, new byte[] {-1}, true)},{
                PChar.INSTANCE.getKeyRange(new byte[] {-1}, true, new byte[] {-1}, true)}},
                new int[] {1, 1},
                ByteUtil.EMPTY_BYTE_ARRAY,
                Bound.UPPER
                ));
        // 20, Variable length
        testCases.addAll(
            foreach(new KeyRange[][]{{
                    PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),},{
                    PVarchar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("B"), true),}},
                new int[] {1,0},
                ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes("aB"), QueryConstants.SEPARATOR_BYTE_ARRAY)),
                Bound.UPPER
                ));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, byte[] expectedKey,
            Bound bound) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, widths, expectedKey, bound});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };
}
