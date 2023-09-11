/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EquiDepthStreamHistogram.Bar;
import org.apache.phoenix.util.EquiDepthStreamHistogram.Bucket;
import org.junit.Before;
import org.junit.Test;

public class EquiDepthStreamHistogramTest {
    byte[] bytesA = Bytes.toBytes("a");
    byte[] bytesB = Bytes.toBytes("b");
    byte[] bytesC = Bytes.toBytes("c");
    byte[] bytesD = Bytes.toBytes("d");
    byte[] bytesE = Bytes.toBytes("e");
    Bar a_b;
    Bar b_c;
    Bar c_d;
    Bar d_e;

    @Before
    public void resetBars() {
        a_b = new Bar(bytesA, bytesB);
        b_c = new Bar(bytesB, bytesC);
        c_d = new Bar(bytesC, bytesD);
        d_e = new Bar(bytesD, bytesE);
    }

    @Test
    public void testComparator() {
        // test ordering
        List<Bar> barList = new ArrayList<>();
        barList.add(b_c);
        barList.add(c_d);
        barList.add(a_b);

        Collections.sort(barList);
        assertEquals(a_b, barList.get(0));
        assertEquals(b_c, barList.get(1));
        assertEquals(c_d, barList.get(2));

        // test when a bar fully contains another
        Bar a_a = new Bar(bytesA, bytesA);
        assertEquals(0, a_b.compareTo(a_a));
        assertEquals(0, a_a.compareTo(a_b));
        assertEquals(1, b_c.compareTo(a_a));
        assertEquals(-1, a_a.compareTo(b_c));
        assertEquals(0, Collections.binarySearch(barList, a_a));
        assertEquals(1, Collections.binarySearch(barList, new Bar(bytesB, bytesB)));
        assertEquals(-4, Collections.binarySearch(barList, new Bar(Bytes.toBytes("e"), Bytes.toBytes("e"))));
        assertEquals(0, a_a.compareTo(a_a));
    }

    @Test
    public void testGetBar() {
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(10);
        Bar bar = histo.getBar(bytesB);
        assertTrue(Arrays.equals(bytesB, bar.getLeftBoundInclusive()));
        assertEquals(1, histo.bars.size());
        assertTrue(bar == histo.getBar(bytesB));
        assertTrue(bar == histo.getBar(bytesA));
        assertTrue(bar == histo.getBar(bytesC));
        assertEquals(1, histo.bars.size());
        assertArrayEquals(bytesA, bar.getLeftBoundInclusive());
        assertArrayEquals(bytesC, bar.getRightBoundExclusive());

        histo.bars = new ArrayList<Bar>();
        histo.bars.add(b_c);
        histo.bars.add(c_d);
        assertEquals(b_c, histo.getBar(bytesB));
        assertEquals(c_d, histo.getBar(bytesC));

        assertTrue(histo.getBar(bytesA) == b_c);
        assertTrue(histo.getBar(bytesE) == c_d);
        assertArrayEquals(bytesA, b_c.getLeftBoundInclusive());
        assertArrayEquals(bytesE, c_d.getRightBoundExclusive());
    }

    @Test
    public void testMergeBars() {
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(2, 1);
        // test merge of two bars
        histo.bars.add(a_b);
        histo.bars.add(b_c);
        histo.bars.add(c_d);
        histo.bars.add(d_e);
        histo.totalCount = 20; // maxBarCount of 1.7 * (10/2) = 17
        a_b.incrementCount(3);
        b_c.incrementCount(2);
        c_d.incrementCount(10);
        d_e.incrementCount(5);
        histo.mergeBars();
        assertEquals(3, histo.bars.size());
        Bar mergedBar = histo.bars.get(0);
        assertEquals(5, mergedBar.getSize());
        assertArrayEquals(bytesA, mergedBar.getLeftBoundInclusive());
        assertArrayEquals(bytesC, mergedBar.getRightBoundExclusive());

        // merge again a_c=5 c_d=10 d_e=5
        histo.mergeBars();
        assertEquals(2, histo.bars.size());
        mergedBar = histo.bars.get(0);
        assertEquals(15, mergedBar.getSize());
        assertArrayEquals(bytesA, mergedBar.getLeftBoundInclusive());
        assertArrayEquals(bytesD, mergedBar.getRightBoundExclusive());

        // a_d=15 d_e=5 , 20 > 17 so merge shouldn't happen
        histo.mergeBars();
        assertEquals(2, histo.bars.size());
    }

    @Test
    public void testSplitBar() {
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(10);
        Bar targetBar = new Bar(bytesA, bytesC);
        targetBar.incrementCount(31);
        histo.bars.add(targetBar);
        histo.splitBar(targetBar);
        assertEquals(2, histo.bars.size());
        Bar newLeft = histo.bars.get(0);
        assertArrayEquals(bytesA, newLeft.getLeftBoundInclusive());
        assertArrayEquals(bytesB, newLeft.getRightBoundExclusive());
        assertEquals(15, newLeft.getSize());
        Bar newRight = histo.bars.get(1);
        assertArrayEquals(bytesB, newRight.getLeftBoundInclusive());
        assertArrayEquals(bytesC, newRight.getRightBoundExclusive());
        assertEquals(16, newRight.getSize());

        // test blocked bars are distributed correctly
        histo.bars.clear();
        targetBar = new Bar(bytesA, bytesE);
        targetBar.incrementCount(10);
        a_b.incrementCount(3);
        targetBar.addBlockedBar(a_b);
        b_c.incrementCount(4);
        targetBar.addBlockedBar(b_c);
        c_d.incrementCount(2);
        targetBar.addBlockedBar(c_d);
        d_e.incrementCount(1);
        targetBar.addBlockedBar(d_e);
        histo.bars.add(targetBar);
        histo.splitBar(targetBar);
        newLeft = histo.bars.get(0);
        newRight = histo.bars.get(1);
        assertEquals(10, newLeft.getSize());
        assertEquals(a_b, newLeft.getBlockedBars().get(0));
        assertEquals(d_e, newLeft.getBlockedBars().get(1));
        assertEquals(10, newRight.getSize());
        assertEquals(b_c, newRight.getBlockedBars().get(0));
        assertEquals(c_d, newRight.getBlockedBars().get(1));
    }

    @Test
    public void testAddValues() {
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(3);
        for (int i = 0; i < 100; i++) {
            histo.addValue(Bytes.toBytes(i + ""));
        }
        // (expansion factor 7) * (3 buckets)
        assertEquals(21, histo.bars.size());
        long total = 0;
        for (Bar b : histo.bars) {
            total += b.getSize();
        }
        assertEquals(100, total);
    }

    @Test
    public void testComputeBuckets() {
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(3);
        histo.addValue(bytesA);
        histo.addValue(bytesB);
        histo.addValue(bytesC);
        histo.addValue(bytesD);
        histo.addValue(bytesE);
        List<Bucket> buckets = histo.computeBuckets();
        assertEquals(3, buckets.size());
        Bucket bucket = buckets.get(0);
        assertEquals(2, bucket.getCountEstimate());
        assertInBucket(bucket, bytesA);
        assertInBucket(bucket, bytesB);
        bucket = buckets.get(1);
        assertEquals(2, bucket.getCountEstimate());
        assertInBucket(bucket, bytesC);
        assertInBucket(bucket, bytesD);
        bucket = buckets.get(2);
        assertEquals(1, bucket.getCountEstimate());
        assertInBucketInclusive(bucket, bytesE);

        // test closestSplitIdx - total count is currently 5, idealBuckSize=2
        histo.bars.clear();
        a_b.incrementCount();
        histo.bars.add(a_b);
        Bar b_d = new Bar(bytesB, bytesD);
        b_d.incrementCount(3); // use 1/3 of this bar's count for first bucket
        histo.bars.add(b_d);
        histo.bars.add(d_e);
        buckets = histo.computeBuckets();
        bucket = buckets.get(0);
        // bound should be 1/3 of [bytesB, bytesD),
        // since we used 1/3 of b_d's count for first bucket
        byte[][] splits = Bytes.split(bytesB, bytesD, 8);
        assertArrayEquals(splits[3], bucket.getRightBoundExclusive());
        bucket = buckets.get(1);
        assertArrayEquals(splits[3], bucket.leftBoundInclusive);
    }

    // check if the value lies in the bucket range
    private void assertInBucket(Bucket bucket, byte[] value) {
        assertTrue(Bytes.compareTo(value, bucket.getLeftBoundInclusive()) >= 0);
        assertTrue(Bytes.compareTo(value, bucket.getRightBoundExclusive()) < 0);
    }

    // right bound is inclusive
    private void assertInBucketInclusive(Bucket bucket, byte[] value) {
        assertTrue(Bytes.compareTo(value, bucket.getLeftBoundInclusive()) >= 0);
        assertTrue(Bytes.compareTo(value, bucket.getRightBoundExclusive()) <= 0);
    }

    /**
     * Stream of data is has uniformly distributed values
     */
    @Test
    public void testUniformDistribution() {
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(4);
        for (int i = 0; i < 100000; i++) {
            histo.addValue(Bytes.toBytes((i % 8) + ""));
        }
        Iterator<Bucket> buckets = histo.computeBuckets().iterator();
        Bucket bucket = buckets.next();
        assertEquals(25000, bucket.getCountEstimate());
        assertInBucket(bucket, Bytes.toBytes("0"));
        assertInBucket(bucket, Bytes.toBytes("1"));
        bucket = buckets.next();
        assertEquals(25000, bucket.getCountEstimate());
        assertInBucket(bucket, Bytes.toBytes("2"));
        assertInBucket(bucket, Bytes.toBytes("3"));
        bucket = buckets.next();
        assertEquals(25000, bucket.getCountEstimate());
        assertInBucket(bucket, Bytes.toBytes("4"));
        assertInBucket(bucket, Bytes.toBytes("5"));
        bucket = buckets.next();
        assertEquals(25000, bucket.getCountEstimate());
        assertInBucket(bucket, Bytes.toBytes("6"));
        assertInBucket(bucket, Bytes.toBytes("7"));
    }

    /**
     * Stream of data is skewed Gaussian distribution with mean of 100 and standard deviation of 25
     */
    @Test
    public void testSkewedDistribution() {
        Random random = new Random();
        EquiDepthStreamHistogram histo = new EquiDepthStreamHistogram(5);
        for (int i = 0; i < 100000; i++) {
            int value = (int) Math.round(random.nextGaussian() * 25 + 100);
            histo.addValue(Bytes.toBytes(value));
        }
        // our middle bucket should have a smaller length than the end buckets,
        // since we have more values clustered in the middle
        List<Bucket> buckets = histo.computeBuckets();
        Bucket first = buckets.get(0);
        int firstLength = getLength(first);
        Bucket last = buckets.get(4);
        int lastLength = getLength(last);
        Bucket middle = buckets.get(2);
        int middleLength = getLength(middle);
        assertTrue(firstLength - middleLength > 25);
        assertTrue(lastLength - middleLength > 25);
    }

    private int getLength(Bucket last) {
        return Math.abs(
            Bytes.toInt(last.getLeftBoundInclusive()) - Bytes.toInt(last.getRightBoundExclusive()));
    }
}
