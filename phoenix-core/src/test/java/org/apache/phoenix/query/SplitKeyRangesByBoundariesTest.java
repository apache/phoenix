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

package org.apache.phoenix.query;

import static java.util.Arrays.asList;

import com.google.common.collect.Lists;
import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class SplitKeyRangesByBoundariesTest extends TestCase {
    private final List<List<KeyRange>> expected;
    private final List<byte[]> boundaries;
    private final List<KeyRange> keyRanges;

    public SplitKeyRangesByBoundariesTest(List<List<KeyRange>> expected, List<byte[]> boundaries,
            List<KeyRange> keyRanges) {
        this.expected = expected;
        this.boundaries = boundaries;
        this.keyRanges = keyRanges;
    }

    @Parameters(name = "{0} {1} {2}") public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();

        // 1, region boundaries is null, return null
        testCases.addAll(Arrays.asList(new Object[][] { { expected(null), boundaries(null),
                keyRanges(PInteger.INSTANCE
                        .getKeyRange(Bytes.toBytes(1), true, Bytes.toBytes(2), false)) } }));

        // 2, key ranges is null, return null
        testCases.addAll(Arrays.asList(new Object[][] {
                { expected(null), boundaries(Bytes.toBytes(2)), keyRanges(null) } }));

        // 3, empty key ranges, return null
        testCases.addAll(Arrays.asList(new Object[][] {
                { expected(null), boundaries(Bytes.toBytes(2)), keyRanges() } }));

        // 4, single regions single key range
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(1,5)
                                ))
                        )),
                        boundaries(),
                        keyRanges(getKeyRange(1, 5))
                } }));

        // 5, single region, multiple key ranges
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(1,2),
                                        getKeyRange(3,5)
                                ))
                        )),
                        boundaries(),
                        keyRanges(
                                getKeyRange(1,2),
                                getKeyRange(3,5)
                        )
                } }));

        // 6, Multiple regions, one key range covers all regions.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(1,2)
                                )),
                                new Pair<Integer, List<KeyRange>> (1, Arrays.asList(
                                        getKeyRange(2,4)
                                )),
                                new Pair<Integer, List<KeyRange>> (2, Arrays.asList(
                                        getKeyRange(4,5)
                                ))
                        )),
                        boundaries(Bytes.toBytes(2), Bytes.toBytes(4)),
                        keyRanges(getKeyRange(1,5))
                } }));

        // 7, Multiple regions, one key range is in the first region.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(1,5)
                                ))
                        )),
                        boundaries(Bytes.toBytes(8), Bytes.toBytes(16), Bytes.toBytes(24)),
                        keyRanges(getKeyRange(1,5))
                } }));

        // 8, Multiple regions, one key range is in a middle region.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (1, Arrays.asList(
                                        getKeyRange(8, 16)
                                ))
                        )),
                        boundaries(Bytes.toBytes(8), Bytes.toBytes(16), Bytes.toBytes(24)),
                        keyRanges(getKeyRange(8, 16))
                } }));

        // 9, Multiple regions, one key range is in the last region. Testing Boundaries.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (3, Arrays.asList(
                                        getKeyRange(24, 32)
                                ))
                        )),
                        boundaries(Bytes.toBytes(8), Bytes.toBytes(16), Bytes.toBytes(24)),
                        keyRanges(getKeyRange(24, 32))
                } }));

        // 10, Multiple regions, one key range (UNBOUND, UNBOUND) covers all regions.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(Integer.MIN_VALUE, 2)
                                )),
                                new Pair<Integer, List<KeyRange>> (1, Arrays.asList(
                                        getKeyRange(2, 4)
                                )),
                                new Pair<Integer, List<KeyRange>> (2, Arrays.asList(
                                        getKeyRange(4, Integer.MAX_VALUE)
                                ))
                        )),
                        boundaries(Bytes.toBytes(2), Bytes.toBytes(4)),
                        keyRanges(getKeyRange(Integer.MIN_VALUE, Integer.MAX_VALUE))
                } }));

        // 11, Multiple regions, multiple key ranges cover all regions.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(7, 8)
                                )),
                                new Pair<Integer, List<KeyRange>> (1, Arrays.asList(
                                        getKeyRange(8, 16)
                                )),
                                new Pair<Integer, List<KeyRange>> (2, Arrays.asList(
                                        getKeyRange(16, 20),
                                        getKeyRange(22, 24)
                                )),
                                new Pair<Integer, List<KeyRange>> (3, Arrays.asList(
                                        getKeyRange(24, Integer.MAX_VALUE)
                                ))
                        )),
                        boundaries(Bytes.toBytes(8), Bytes.toBytes(16), Bytes.toBytes(24)),
                        keyRanges(
                                getKeyRange(7, 20),
                                getKeyRange(22, Integer.MAX_VALUE)
                        )
                } }));

        // 12, Multiple regions, multiple key ranges cover part regions with cavities and some key ranges are in the same region. Testing boundaries.
        testCases.addAll(Arrays.asList(new Object[][] {
                {
                        expected(Arrays.asList(
                                new Pair<Integer, List<KeyRange>> (0, Arrays.asList(
                                        getKeyRange(2, 4),
                                        getKeyRange(4, 8)
                                )),
                                new Pair<Integer, List<KeyRange>> (2, Arrays.asList(
                                        getKeyRange(16, 18),
                                        getKeyRange(18, 20),
                                        getKeyRange(20, 24)
                                ))
                        )),
                        boundaries(Bytes.toBytes(8), Bytes.toBytes(16), Bytes.toBytes(24)),
                        keyRanges(
                                getKeyRange(2, 4),
                                getKeyRange(4, 8),
                                getKeyRange(16, 18),
                                getKeyRange(18, 20),
                                getKeyRange(20, 24)
                        )
                } }));

        return testCases;
    }

    @Test public void test() {
        List<Pair<Integer, List<KeyRange>>> ret = ScanUtil.splitKeyRangesByBoundaries(boundaries, keyRanges);
        assertEquals(expected, ret);
    }

    /**
     * Generate KeyRange with the given lower Bound (always inclusive) and upper Bound (always exclusive)
     * @param lowerBound
     * @param upperBound
     * @return
     */
    private static KeyRange getKeyRange(int lowerBound, int upperBound) {
        return PInteger.INSTANCE.getKeyRange(
                lowerBound == Integer.MIN_VALUE ? KeyRange.UNBOUND : Bytes.toBytes(lowerBound), true,
                upperBound == Integer.MAX_VALUE ? KeyRange.UNBOUND : Bytes.toBytes(upperBound), false);
    }

    private static final List<Pair<Integer, List<KeyRange>>> expected(
            List<Pair<Integer, List<KeyRange>>> expectedValue) {
        return expectedValue;
    }
    private static final List<byte[]> boundaries(byte[]... bytes) {
        return bytes == null ? null : (bytes.length == 0 ? Lists.<byte[]>newArrayList() : asList(bytes));
    }

    private static final List<KeyRange> keyRanges(KeyRange... kr) {
        return kr == null ? null : (kr.length == 0 ? Lists.<KeyRange>newArrayList() : asList(kr));
    }
}
