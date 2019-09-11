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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;

import junit.framework.TestCase;

public class KeyRangeMoreTest extends TestCase {

    @Test
    public void testListIntersectWithOneResultRange() throws Exception {
        for(boolean addEmptyRange : new boolean[]{true,false}) {
            doTestListIntersectWithOneResultRange(0,200,3,1,180,2,addEmptyRange);
            doTestListIntersectWithOneResultRange(1,180,2,0,200,3,addEmptyRange);

            doTestListIntersectWithOneResultRange(1,180,3,0,200,2,addEmptyRange);
            doTestListIntersectWithOneResultRange(0,200,2,1,180,3,addEmptyRange);

            doTestListIntersectWithOneResultRange(0, 200, 3, 1, 180, 100,addEmptyRange);
            doTestListIntersectWithOneResultRange(1, 180, 100,0, 200, 3,addEmptyRange);

            doTestListIntersectWithOneResultRange(1, 180, 3, 0, 200, 100,addEmptyRange);
            doTestListIntersectWithOneResultRange(0, 200, 100,1, 180, 3,addEmptyRange);
        }
    }

    private void doTestListIntersectWithOneResultRange(int start1,int end1,int step1,int start2,int end2,int step2,boolean addEmptyRange) throws Exception {
        List<KeyRange> rowKeyRanges1=new ArrayList<KeyRange>();
        List<KeyRange> rowKeyRanges2=new ArrayList<KeyRange>();
        for(int i=start1;i<=end1;i++) {
            rowKeyRanges1.add(
                    PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(i), true, PInteger.INSTANCE.toBytes(i+step1), true));

        }
        if(addEmptyRange) {
            rowKeyRanges1.add(KeyRange.EMPTY_RANGE);
        }
        for(int i=start2;i<=end2;i++) {
            rowKeyRanges2.add(
                    PInteger.INSTANCE.getKeyRange(PInteger.INSTANCE.toBytes(i), true, PInteger.INSTANCE.toBytes(i+step2), true));
        }
        if(addEmptyRange) {
            rowKeyRanges2.add(KeyRange.EMPTY_RANGE);
        }
        int maxStart=Math.max(start1, start2);
        int minEnd=Math.min(end1+step1, end2+step2);

        List<KeyRange> expected=Arrays.asList(KeyRange.getKeyRange(
                        PInteger.INSTANCE.toBytes(maxStart),
                        true,
                        PInteger.INSTANCE.toBytes(minEnd),
                        true));

        listIntersectAndAssert(rowKeyRanges1,rowKeyRanges2,expected);
    }

    @Test
    public void testListIntersectWithMultiResultRange() throws Exception {
        for(boolean addEmptyRange : new boolean[]{true,false}) {
            doTestListIntersectWithMultiResultRange(1, 100, 3, 4, 120, 6,addEmptyRange);
            doTestListIntersectWithMultiResultRange(4, 120, 6,1, 100, 3,addEmptyRange);

            doTestListIntersectWithMultiResultRange(1, 200, 3, 5, 240, 10,addEmptyRange);
            doTestListIntersectWithMultiResultRange(5, 240, 10,1, 200, 3,addEmptyRange);
        }

    }

    private void doTestListIntersectWithMultiResultRange(int start1,int count1,int step1,int start2,int count2,int step2,boolean addEmptyRange) throws Exception {
        List<KeyRange> rowKeyRanges1=new ArrayList<KeyRange>();
        List<KeyRange> rowKeyRanges2=new ArrayList<KeyRange>();
        for(int i=1;i<=count1;i++) {
            rowKeyRanges1.add(
                    PInteger.INSTANCE.getKeyRange(
                            PInteger.INSTANCE.toBytes(start1+(i-1)*(step1+1)),
                            true,
                            PInteger.INSTANCE.toBytes(start1+i*(step1+1)-1),
                            true));

        }
        if(addEmptyRange) {
            rowKeyRanges1.add(KeyRange.EMPTY_RANGE);
        }
        for(int i=1;i<=count2;i++) {
            rowKeyRanges2.add(
                    PInteger.INSTANCE.getKeyRange(
                            PInteger.INSTANCE.toBytes(start2+(i-1)*(step2+1)),
                            true,
                            PInteger.INSTANCE.toBytes(start2+i*(step2+1)-1),
                            true));
        }
        if(addEmptyRange) {
            rowKeyRanges2.add(KeyRange.EMPTY_RANGE);
        }
        int maxStart=Math.max(start1, start2);
        int minEnd=Math.min(start1+count1*(step1+1)-1, start2+count2*(step2+1)-1);

        for(int i=0;i<200;i++) {
            List<KeyRange> result=KeyRange.intersect(rowKeyRanges1, rowKeyRanges2);
            assertResult(result, maxStart,minEnd);
            result=KeyRange.intersect(rowKeyRanges2, rowKeyRanges1);
            assertResult(result, maxStart,minEnd);
            Collections.shuffle(rowKeyRanges1);
            Collections.shuffle(rowKeyRanges2);
        }
    }

    private void assertResult(List<KeyRange> result,int start,int end) {
        int expectStart=start;
        for(KeyRange rowKeyRange : result) {
            byte[] lowerRange=rowKeyRange.getLowerRange();
            assertTrue(Bytes.equals(lowerRange, PInteger.INSTANCE.toBytes(expectStart)));
            byte[] upperRange=rowKeyRange.getUpperRange();
            expectStart=((Integer)PInteger.INSTANCE.toObject(upperRange)).intValue()+1;
        }
        assertTrue(expectStart-1==end);
    }

    @Test
    public void testListIntersectForPoint() throws Exception {
        for(boolean addEmptyRange : new boolean[]{true,false}) {
            List<KeyRange> rowKeyRanges1=new ArrayList<KeyRange>();
            List<KeyRange> rowKeyRanges2=new ArrayList<KeyRange>();
            for(int i=0;i<=300;i+=2) {
                rowKeyRanges1.add(
                        KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(i)));
            }
            if(addEmptyRange) {
                rowKeyRanges1.add(KeyRange.EMPTY_RANGE);
            }
            for(int i=0;i<=300;i+=3) {
                rowKeyRanges2.add(
                        KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(i)));
            }
            if(addEmptyRange) {
                rowKeyRanges2.add(KeyRange.EMPTY_RANGE);
            }

            List<KeyRange> expected=new ArrayList<KeyRange>();
            for(int i=0;i<=300;i+=6) {
                expected.add(
                        KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(i)));
            }
            listIntersectAndAssert(rowKeyRanges1,rowKeyRanges2,expected);
        }
    }

    @Test
    public void testListIntersectForBoundary() throws Exception {
        List<KeyRange> rowKeyRanges1=Arrays.asList(KeyRange.EVERYTHING_RANGE);
        List<KeyRange> rowKeyRanges2=new ArrayList<KeyRange>();
        for(int i=0;i<=100;) {
            rowKeyRanges2.add(
                    PInteger.INSTANCE.getKeyRange(
                            PInteger.INSTANCE.toBytes(i),
                            true,
                            PInteger.INSTANCE.toBytes(i+2),
                            true));
            i+=4;
        }
        List<KeyRange> expected=new ArrayList<KeyRange>(rowKeyRanges2);
        listIntersectAndAssert(rowKeyRanges1, rowKeyRanges2, expected);

        rowKeyRanges1=Arrays.asList(KeyRange.EMPTY_RANGE);
        rowKeyRanges2=new ArrayList<KeyRange>(expected);
        listIntersectAndAssert(rowKeyRanges1, rowKeyRanges2, Arrays.asList(KeyRange.EMPTY_RANGE));

        listIntersectAndAssert(Arrays.asList(KeyRange.EMPTY_RANGE),Arrays.asList(KeyRange.EVERYTHING_RANGE),Arrays.asList(KeyRange.EMPTY_RANGE));

        rowKeyRanges1 = createKeyRangeList(
                Arrays.asList(2, 5, 8, Integer.MAX_VALUE),
                Arrays.asList(true, true, true, false));
        rowKeyRanges2 = createKeyRangeList(
                Arrays.asList(Integer.MIN_VALUE, 4, 7, 10, 13, 14, 19, Integer.MAX_VALUE),
                Arrays.asList(false, true, true, true, true, true, true, false));
        expected = createKeyRangeList(
                Arrays.asList(2, 4, 8, 10, 13, 14, 19, Integer.MAX_VALUE),
                Arrays.asList(true, true, true, true, true, true, true, false));
        listIntersectAndAssert(rowKeyRanges1, rowKeyRanges2, expected);

        rowKeyRanges1 = createKeyRangeList(
                Arrays.asList(3, 5, 5, 6),
                Arrays.asList(true, false, true, false));
        rowKeyRanges2 = createKeyRangeList(
                Arrays.asList(3, 5, 6, 7),
                Arrays.asList(true, true, true, true));
        expected = createKeyRangeList(
                Arrays.asList(3, 5),
                Arrays.asList(true, true));
        listIntersectAndAssert(rowKeyRanges1, rowKeyRanges2, expected);
    }

    @Test
    public void testKeyRangeCompareUpperRange() throws Exception {
        List<KeyRange> rowKeyRanges1 = createKeyRangeListWithFixedLowerRange(
                Arrays.asList(Integer.MAX_VALUE, Integer.MAX_VALUE, 10000, 1001, 1000, 1000, 1000, 1000, 1000),
                Arrays.asList(false, false, true, true, true, true, false, true, false));
        List<KeyRange> rowKeyRanges2 = createKeyRangeListWithFixedLowerRange(
                Arrays.asList(Integer.MAX_VALUE, 10000, Integer.MAX_VALUE, 1000, 1001, 1000, 1000, 1000, 1000),
                Arrays.asList(false, false, false, true, true, true, false, false, true));
        List<Integer> expectedResults = Arrays.asList(0, 1, -1, 1, -1, 0, 0, 1, -1);
        assertEquals(rowKeyRanges1.size(), rowKeyRanges2.size());
        assertEquals(rowKeyRanges1.size(), expectedResults.size());

        for (int i = 0; i < expectedResults.size(); i++) {
            int compareResult = KeyRange.compareUpperRange(rowKeyRanges1.get(i), rowKeyRanges2.get(i));
            assertEquals(expectedResults.get(i).intValue(), compareResult);
        }
    }

    private static List<KeyRange> createKeyRangeListWithFixedLowerRange(List<Integer> keys, List<Boolean> boundaryConditions) {
        assertEquals(keys.size(), boundaryConditions.size());
        List<Integer> newKeys = Lists.newArrayListWithCapacity(keys.size() * 2);
        List<Boolean> newBoundaryConditions = Lists.newArrayListWithCapacity(boundaryConditions.size() * 2);

        for (int i = 0; i < keys.size(); i++) {
            newKeys.add(0);
            newBoundaryConditions.add(true);
            newKeys.add(keys.get(i));
            newBoundaryConditions.add(boundaryConditions.get(i));
        }

        return createKeyRangeList(newKeys, newBoundaryConditions);
    }

    private static List<KeyRange> createKeyRangeList(List<Integer> keys, List<Boolean> boundaryConditions) {
        assertEquals(keys.size(), boundaryConditions.size());
        assertTrue(keys.size() % 2 == 0);

        int size = keys.size() / 2;
        List<KeyRange> keyRangeList = Lists.newArrayListWithCapacity(size);

        for (int i = 0; i < size; i++) {
            byte[] startKey = keys.get(2*i).equals(Integer.MIN_VALUE) ? KeyRange.UNBOUND : PInteger.INSTANCE.toBytes(keys.get(2*i));
            byte[] endKey = keys.get(2*i + 1).equals(Integer.MAX_VALUE) ? KeyRange.UNBOUND : PInteger.INSTANCE.toBytes(keys.get(2*i + 1));
            keyRangeList.add(PInteger.INSTANCE.getKeyRange(startKey, boundaryConditions.get(2*i), endKey, boundaryConditions.get(2*i+1)));
        }

        return keyRangeList;
    }

    private static void listIntersectAndAssert(List<KeyRange> rowKeyRanges1,List<KeyRange> rowKeyRanges2,List<KeyRange> expected) {
        for (int i = 0; i < 200; i++) {
            List<KeyRange> result = KeyRange.intersect(rowKeyRanges1, rowKeyRanges2);
            assertEquals(expected, result);
            result = KeyRange.intersect(rowKeyRanges2, rowKeyRanges1);
            assertEquals(expected, result);
            Collections.shuffle(rowKeyRanges1);
            Collections.shuffle(rowKeyRanges2);
        }
    }
}
