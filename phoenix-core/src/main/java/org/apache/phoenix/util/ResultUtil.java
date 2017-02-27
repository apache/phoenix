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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Static class for various methods that would be nice to have added to {@link org.apache.hadoop.hbase.client.Result}.
 * These methods work off of the raw bytes preventing the explosion of Result into object form.
 * 
 * 
 * @since 0.1
 */
public class ResultUtil {
    public static final Result EMPTY_RESULT = new Result() {
        @Override
        public final boolean isEmpty() { return true; }
    };
    
    private ResultUtil() {
    }
    
    public static Result toResult(ImmutableBytesWritable bytes) {
        byte [] buf = bytes.get();
        int offset = bytes.getOffset();
        int finalOffset = bytes.getSize() + offset;
        List<Cell> kvs = new ArrayList<Cell>();
        while(offset < finalOffset) {
          int keyLength = Bytes.toInt(buf, offset);
          offset += Bytes.SIZEOF_INT;
          kvs.add(new KeyValue(buf, offset, keyLength));
          offset += keyLength;
        }
        return Result.create(kvs);
    }
    
    /**
     * Return a pointer into a potentially much bigger byte buffer that points to the key of a Result.
     * @param r
     */
    public static ImmutableBytesWritable getKey(Result r) {
        return getKey(r, 0);
    }
    
    public static void getKey(Result r, ImmutableBytesWritable key) {
        key.set(r.getRow());
        //key.set(getRawBytes(r), getKeyOffset(r), getKeyLength(r));
    }
    
    @SuppressWarnings("deprecation")
    public static void getKey(KeyValue value, ImmutableBytesWritable key) {
        key.set(value.getBuffer(), value.getRowOffset(), value.getRowLength());
    }
    
    /**
     * Return a pointer into a potentially much bigger byte buffer that points to the key of a Result.
     * Use offset to return a subset of the key bytes, for example to skip the organization ID embedded
     * in all of our keys.
     * @param r
     * @param offset offset added to start of key and subtracted from key length (to select subset of key bytes)
     */
    public static ImmutableBytesWritable getKey(Result r, int offset) {
        return new ImmutableBytesWritable(getRawBytes(r), getKeyOffset(r) + offset, getKeyLength(r) - offset);
    }

    public static void getKey(Result r, int offset, int length, ImmutableBytesWritable key) {
        key.set(getRawBytes(r), getKeyOffset(r) + offset, length);
    }

    /**
     * Comparator for comparing the keys from two Results in-place, without allocating new byte arrays
     */
    public static final Comparator<Result> KEY_COMPARATOR = new Comparator<Result>() {

        @Override
        public int compare(Result r1, Result r2) {
            byte[] r1Bytes = getRawBytes(r1);
            byte[] r2Bytes = getRawBytes(r2);
            return Bytes.compareTo(r1Bytes, getKeyOffset(r1), getKeyLength(r1), r2Bytes, getKeyOffset(r2), getKeyLength(r2));
        }
        
    };
    
    /**
     * Get the offset into the Result byte array to the key.
     * @param r
     */
    static int getKeyOffset(Result r) {
        KeyValue firstKV = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(r.rawCells()[0]);
        return firstKV.getOffset();
    }
    
    static int getKeyLength(Result r) {
        // Key length stored right before key as a short
        return Bytes.toShort(getRawBytes(r), getKeyOffset(r) - Bytes.SIZEOF_SHORT);
    }
    
    @SuppressWarnings("deprecation")
    static byte[] getRawBytes(Result r) {
        KeyValue firstKV = org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(r.rawCells()[0]);
        return firstKV.getBuffer();
    }

    public static int compareKeys(Result r1, Result r2) {
        return Bytes.compareTo(getRawBytes(r1), getKeyOffset(r1), getKeyLength(r1), getRawBytes(r2), getKeyOffset(r2), getKeyLength(r2));
    }

}
