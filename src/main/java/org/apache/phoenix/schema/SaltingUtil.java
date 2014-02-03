/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.phoenix.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.KeyRange.Bound;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;


/**
 * Utility methods related to transparent salting of row keys.
 */
public class SaltingUtil {
    public static final int NUM_SALTING_BYTES = 1;
    public static final Integer MAX_BUCKET_NUM = 256; // Unsigned byte.
    public static final String SALTING_COLUMN_NAME = "_SALT";
    public static final String SALTED_ROW_KEY_NAME = "_SALTED_KEY";
    public static final PColumnImpl SALTING_COLUMN = new PColumnImpl(
            PNameFactory.newName(SALTING_COLUMN_NAME), null, PDataType.BINARY, 1, 0, false, 0, null);

    public static List<KeyRange> generateAllSaltingRanges(int bucketNum) {
        List<KeyRange> allRanges = Lists.<KeyRange>newArrayListWithExpectedSize(bucketNum);
        for (int i=0; i<bucketNum; i++) {
            byte[] saltByte = new byte[] {(byte) i};
            allRanges.add(SALTING_COLUMN.getDataType().getKeyRange(
                    saltByte, true, saltByte, true));
        }
        return allRanges;
    }

    public static byte[][] getSalteByteSplitPoints(int saltBucketNum) {
        byte[][] splits = new byte[saltBucketNum-1][];
        for (int i = 1; i < saltBucketNum; i++) {
            splits[i-1] = new byte[] {(byte) i};
        }
        return splits;
    }

    // Compute the hash of the key value stored in key and set its first byte as the value. The
    // first byte of key should be left empty as a place holder for the salting byte.
    public static byte[] getSaltedKey(ImmutableBytesWritable key, int bucketNum) {
        byte[] keyBytes = new byte[key.getLength()];
        byte saltByte = getSaltingByte(key.get(), key.getOffset() + 1, key.getLength() - 1, bucketNum);
        keyBytes[0] = saltByte;
        System.arraycopy(key.get(), key.getOffset() + 1, keyBytes, 1, key.getLength() - 1);
        return keyBytes;
    }

    // Generate the bucket byte given a byte array and the number of buckets.
    public static byte getSaltingByte(byte[] value, int offset, int length, int bucketNum) {
        int hash = hashCode(value, offset, length);
        byte bucketByte = (byte) ((Math.abs(hash) % bucketNum));
        return bucketByte;
    }

    private static int hashCode(byte a[], int offset, int length) {
        if (a == null)
            return 0;
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    public static List<List<KeyRange>> setSaltByte(List<List<KeyRange>> ranges, int bucketNum) {
        if (ranges == null || ranges.isEmpty()) {
            return ScanRanges.NOTHING.getRanges();
        }
        for (int i = 1; i < ranges.size(); i++) {
            List<KeyRange> range = ranges.get(i);
            if (range != null && !range.isEmpty()) {
                throw new IllegalStateException();
            }
        }
        List<KeyRange> newRanges = Lists.newArrayListWithExpectedSize(ranges.size());
        for (KeyRange range : ranges.get(0)) {
            if (!range.isSingleKey()) {
                throw new IllegalStateException();
            }
            byte[] key = range.getLowerRange();
            byte saltByte = SaltingUtil.getSaltingByte(key, 0, key.length, bucketNum);
            byte[] saltedKey = new byte[key.length + 1];
            System.arraycopy(key, 0, saltedKey, 1, key.length);   
            saltedKey[0] = saltByte;
            newRanges.add(KeyRange.getKeyRange(saltedKey, true, saltedKey, true));
        }
        return Collections.singletonList(newRanges);
    }
    
    public static List<List<KeyRange>> flattenRanges(List<List<KeyRange>> ranges, RowKeySchema schema, int bucketNum) {
        if (ranges == null || ranges.isEmpty()) {
            return ScanRanges.NOTHING.getRanges();
        }
        int count = 1;
        // Skip salt byte range in the first position
        for (int i = 1; i < ranges.size(); i++) {
            count *= ranges.get(i).size();
        }
        KeyRange[] expandedRanges = new KeyRange[count];
        int[] position = new int[ranges.size()];
        int maxKeyLength = SchemaUtil.getMaxKeyLength(schema, ranges);
        int idx = 0, length;
        byte saltByte;
        byte[] key = new byte[maxKeyLength];
        do {
            length = ScanUtil.setKey(schema, ranges, position, Bound.LOWER, key, NUM_SALTING_BYTES, 1, ranges.size(), 1);
            saltByte = SaltingUtil.getSaltingByte(key, 1, length, bucketNum);
            key[0] = saltByte;
            byte[] saltedStartKey = Arrays.copyOf(key, length + 1);
            length = ScanUtil.setKey(schema, ranges, position, Bound.UPPER, key, NUM_SALTING_BYTES, 1, ranges.size(), 1);
            byte[] saltedEndKey = Arrays.copyOf(key, length + 1);
            KeyRange range = PDataType.VARBINARY.getKeyRange(saltedStartKey, true, saltedEndKey, false);
            expandedRanges[idx++] = range;
        } while (incrementKey(ranges, position));
        // The comparator is imperfect, but sufficient for all single keys.
        Arrays.sort(expandedRanges, KeyRange.COMPARATOR);
        List<KeyRange> expandedRangesList = Arrays.asList(expandedRanges);
        return Collections.singletonList(expandedRangesList);
    }

    private static boolean incrementKey(List<List<KeyRange>> slots, int[] position) {
        int idx = slots.size() - 1;
        while (idx >= 0 && (position[idx] = (position[idx] + 1) % slots.get(idx).size()) == 0) {
            idx--;
        }
        return idx >= 0;
    }

    public static KeyRange addSaltByte(byte[] startKey, KeyRange minMaxRange) {
        byte saltByte = startKey.length == 0 ? 0 : startKey[0];
        byte[] lowerRange = minMaxRange.getLowerRange();
        if(!minMaxRange.lowerUnbound()) {
            byte[] newLowerRange = new byte[lowerRange.length + 1];
            newLowerRange[0] = saltByte;
            System.arraycopy(lowerRange, 0, newLowerRange, 1, lowerRange.length);
            lowerRange = newLowerRange;
        }
        byte[] upperRange = minMaxRange.getUpperRange();

        if(!minMaxRange.upperUnbound()) { 
            byte[] newUpperRange = new byte[upperRange.length + 1];
            newUpperRange[0] = saltByte;
            System.arraycopy(upperRange, 0, newUpperRange, 1, upperRange.length);
            upperRange = newUpperRange;
        }
        return KeyRange.getKeyRange(lowerRange, upperRange);
    }
}
