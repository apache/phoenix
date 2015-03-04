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
package org.apache.phoenix.schema;

import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;


/**
 * Utility methods related to transparent salting of row keys.
 */
public class SaltingUtil {
    public static final int NUM_SALTING_BYTES = 1;
    public static final Integer MAX_BUCKET_NUM = 256; // Unsigned byte.
    public static final String SALTING_COLUMN_NAME = "_SALT";
    public static final String SALTED_ROW_KEY_NAME = "_SALTED_KEY";
    public static final PColumnImpl SALTING_COLUMN = new PColumnImpl(
            PNameFactory.newName(SALTING_COLUMN_NAME), null, PBinary.INSTANCE, 1, 0, false, 0, SortOrder.getDefault(), 0, null, false, null);
    public static final RowKeySchema VAR_BINARY_SALTED_SCHEMA = new RowKeySchemaBuilder(2)
        .addField(SALTING_COLUMN, false, SortOrder.getDefault())
        .addField(SchemaUtil.VAR_BINARY_DATUM, false, SortOrder.getDefault()).build();

    public static List<KeyRange> generateAllSaltingRanges(int bucketNum) {
        List<KeyRange> allRanges = Lists.newArrayListWithExpectedSize(bucketNum);
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
        int hash = calculateHashCode(value, offset, length);
        return (byte) Math.abs(hash % bucketNum);
    }

    private static int calculateHashCode(byte a[], int offset, int length) {
        if (a == null)
            return 0;
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
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

    public static void addRegionStartKeyToScanStartAndStopRows(byte[] startKey, byte[] endKey, Scan scan) {
        if (startKey.length == 0 && endKey.length == 0) return;
        byte[] prefixBytes = startKey.length != 0 ? startKey : new byte[endKey.length];
        byte[] newStartRow = new byte[scan.getStartRow().length + prefixBytes.length];
        System.arraycopy(prefixBytes, 0, newStartRow, 0, prefixBytes.length);
        System.arraycopy(scan.getStartRow(), 0, newStartRow, prefixBytes.length, scan.getStartRow().length);
        scan.setStartRow(newStartRow);
        if (scan.getStopRow().length != 0) {
            byte[] newStopRow = new byte[scan.getStopRow().length + prefixBytes.length];
            System.arraycopy(prefixBytes, 0, newStopRow, 0, prefixBytes.length);
            System.arraycopy(scan.getStopRow(), 0, newStopRow, prefixBytes.length, scan.getStopRow().length);
            scan.setStopRow(newStopRow);
        }
    }
}
