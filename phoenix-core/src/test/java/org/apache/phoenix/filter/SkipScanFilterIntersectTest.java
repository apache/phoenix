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
package org.apache.phoenix.filter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


/**
 * Test for intersect method in {@link SkipScanFilter}
 */
@RunWith(Parameterized.class)
public class SkipScanFilterIntersectTest {

    private final SkipScanFilter filter;
    private final byte[] lowerInclusiveKey;
    private final byte[] upperExclusiveKey;
    private final List<List<KeyRange>> expectedNewSlots;

    public SkipScanFilterIntersectTest(List<List<KeyRange>> slots, RowKeySchema schema, byte[] lowerInclusiveKey,
            byte[] upperExclusiveKey, List<List<KeyRange>> expectedNewSlots) {
        this.filter = new SkipScanFilter(slots, schema, false);
        this.lowerInclusiveKey = lowerInclusiveKey;
        this.upperExclusiveKey = upperExclusiveKey;
        this.expectedNewSlots = expectedNewSlots;
    }

    @Test
    public void test() {
        SkipScanFilter intersectedFilter = filter.intersect(lowerInclusiveKey, upperExclusiveKey);
        if (expectedNewSlots == null && intersectedFilter == null) {
            return;
        }
        assertNotNull("Intersected filter should not be null", intersectedFilter);
        List<List<KeyRange>> newSlots = intersectedFilter.getSlots();
        assertSameSlots(expectedNewSlots, newSlots);
    }

    private void assertSameSlots(List<List<KeyRange>> expectedSlots, List<List<KeyRange>> slots) {
        assertEquals(expectedSlots.size(), slots.size());
        for (int i=0; i<expectedSlots.size(); i++) {
            List<KeyRange> expectedSlot = expectedSlots.get(i);
            List<KeyRange> slot = slots.get(i);
            assertEquals("index: " + i, expectedSlot.size(), slot.size());
            for (int j=0; j<expectedSlot.size(); j++) {
                KeyRange expectedRange = expectedSlot.get(j);
                KeyRange range = slot.get(j);
                assertArrayEquals(expectedRange.getLowerRange(), range.getLowerRange());
                assertArrayEquals(expectedRange.getUpperRange(), range.getUpperRange());
                assertEquals(expectedRange.isLowerInclusive(), range.isLowerInclusive());
                assertEquals(expectedRange.isUpperInclusive(), range.isUpperInclusive());
            }
        }
    }

    @Parameters(name = "{0} {4}")
    public static synchronized Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // Both ranges in second slot are required b/c first slot contains range and upper/lower
        // values differ in this slot position.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("e"),
                        false, SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"),
                                false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("j3A"), Bytes.toBytes("k4C"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"),
                        false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC), } }));
        // Only second range in second slot is required b/c though first slot contains range,
        // upper/lower values do not differ in this slot position.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("e"),
                        false, SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"),
                                false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("j3A"), Bytes.toBytes("j4C"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"),
                        false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC), } }));
        // Test case exercising repositioning multiple times (initially to slot #2 and then again
        // to slot #4). Because there's a range for slot #4 and the lower/upper values are
        // different,
        // all slot #5 ranges are part of the intersection.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("d"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"),
                        false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("C"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("m"), true, Bytes.toBytes("u"),
                        false, SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("z"), true, Bytes.toBytes("z"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("D"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("M"), true, Bytes.toBytes("M"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1, 1, 1 }, Bytes.toBytes("bkCpM"), Bytes.toBytes("bkCtD"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("j"), true, Bytes.toBytes("m"),
                        false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("C"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("m"), true, Bytes.toBytes("u"),
                        false, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("D"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("M"), true, Bytes.toBytes("M"),
                                true, SortOrder.ASC), } }));
        // Single matching in the first 2 slots.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("b1B"), Bytes.toBytes("b1C"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC), } }));
        // Single matching in the first slot.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("b1Z"), Bytes.toBytes("b3Z"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        // No overlap
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("2"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("G"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a1I"), Bytes.toBytes("a2A"), null));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0A"), Bytes.toBytes("b1B"), null));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0A"), Bytes.toBytes("b1C"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0A"), Bytes.toBytes("b1D"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0A"), Bytes.toBytes("b1D"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("b1B"), Bytes.toBytes("b1D"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("d"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("3"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0A"), Bytes.toBytes("b1F"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0Z"), Bytes.toBytes("b3Z"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0Z"), Bytes.toBytes("b9Z"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        // Multiple matching in all slot.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0Z"), Bytes.toBytes("c3Z"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, 1 }, Bytes.toBytes("a0A"), Bytes.toBytes("f4F"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        // VARCHAR as the last column, various cases.
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, -1 }, Bytes.toBytes("d3AA"), Bytes.toBytes("d4FF"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC) } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, -1 }, Bytes.toBytes("d0AA"), Bytes.toBytes("d4FF"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"), true,
                        SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        testCases.addAll(foreach(
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } },
            new int[] { 1, 1, -1 }, Bytes.toBytes("a0AA"), Bytes.toBytes("f4FF"),
            new KeyRange[][] {
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("c"), true, Bytes.toBytes("e"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("4"),
                                true, SortOrder.ASC), },
                    { PChar.INSTANCE.getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("B"), true,
                        SortOrder.ASC),
                            PChar.INSTANCE.getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("E"),
                                true, SortOrder.ASC), } }));
        return testCases;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, byte[] lowerInclusive,
            byte[] upperExclusive, KeyRange[][] expectedRanges) {
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        List<List<KeyRange>> expectedSlots = expectedRanges == null ? null : Lists.transform(Lists.newArrayList(expectedRanges), ARRAY_TO_LIST);
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(10);
        for (final int width: widths) {
            builder.addField(
                    new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return width <= 0;
                        }
                        @Override
                        public PDataType getDataType() {
                            return width <= 0 ? PVarchar.INSTANCE : PChar.INSTANCE;
                        }
                       @Override
                        public Integer getMaxLength() {
                            return width <= 0 ? null : width;
                        }
                        @Override
                        public Integer getScale() {
                            return null;
                        }
                        @Override
                        public SortOrder getSortOrder() {
                            return SortOrder.getDefault();
                        }
                    }, width <= 0, SortOrder.getDefault());
        }
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {slots, builder.build(), lowerInclusive, upperExclusive, expectedSlots});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = new Function<KeyRange[], List<KeyRange>>() {
        @Override public List<KeyRange> apply(KeyRange[] input) {
            return Lists.newArrayList(input);
        }
    };
}
