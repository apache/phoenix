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


/**
 * Utilities for computing an object's size.  All size measurements are in bytes.
 * Note, all of the sizes here, but especially OBJECT_SIZE and ARRAY_SIZE are estimates and will
 * depend on the JVM itself (and which JVM, 64bit vs. 32bit, etc).
 * The current values are based on:
 * Java HotSpot(TM) 64-Bit Server VM/14.2-b01
 * 
 * (Uncomment out and run the main w/ appropriate object to test)
 * Also, see this link:
 * https://sites.google.com/a/salesforce.com/development/Home/old-wiki-home-page/i-wish-i-knew#TOC-How-to-measure-the-size-of-a-Java-O
 * For another way to measure.
 */
public class SizedUtil {
    public static final int POINTER_SIZE = 8; // 64 bit jvm.
    public static final int OBJECT_SIZE = 16; // measured, see class comment.
    public static final int ARRAY_SIZE = 24; // measured, see class comment.
    public static final int CHAR_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    
    public static final int TREE_MAP_SIZE = OBJECT_SIZE + INT_SIZE * 2 + POINTER_SIZE * 2;
    public static final int MAP_ENTRY_SIZE = OBJECT_SIZE + 3 * POINTER_SIZE + INT_SIZE;
    public static final int IMMUTABLE_BYTES_WRITABLE_SIZE = OBJECT_SIZE + INT_SIZE * 2 + ARRAY_SIZE;
    public static final int IMMUTABLE_BYTES_PTR_SIZE = IMMUTABLE_BYTES_WRITABLE_SIZE + INT_SIZE;// Extra is an int field which caches hashcode.
    public static final int KEY_VALUE_SIZE = 2 * INT_SIZE + LONG_SIZE + 2 * ARRAY_SIZE;
    public static final int RESULT_SIZE = OBJECT_SIZE +  3 * POINTER_SIZE + IMMUTABLE_BYTES_WRITABLE_SIZE;
    public static final int INT_OBJECT_SIZE = INT_SIZE + OBJECT_SIZE;
    public static final int LONG_OBJECT_SIZE = LONG_SIZE + OBJECT_SIZE;
    public static final int BIG_DECIMAL_SIZE = 
        OBJECT_SIZE + 2 * INT_SIZE + LONG_SIZE + 2 * POINTER_SIZE +
        OBJECT_SIZE /* BigInteger */ + 5 * INT_SIZE + ARRAY_SIZE /*mag[]*/ + 2 * INT_SIZE /* est mag[2] */;

    private SizedUtil() {
    }
    
    public static int sizeOfTreeMap(int size) {
        return TREE_MAP_SIZE + (OBJECT_SIZE + INT_SIZE + POINTER_SIZE * 5) * size;
    }
    
    public static int sizeOfArrayList(int capacity) {
        return SizedUtil.OBJECT_SIZE + SizedUtil.POINTER_SIZE + SizedUtil.INT_SIZE + SizedUtil.ARRAY_SIZE + SizedUtil.POINTER_SIZE * capacity;
    }
    
    public static long sizeOfMap(int nRows) {
        return sizeOfMap(nRows, SizedUtil.POINTER_SIZE, SizedUtil.POINTER_SIZE);
    }
    
    public static long sizeOfMap(int nRows, int keySize, int valueSize) {
        return SizedUtil.OBJECT_SIZE * 4 + sizeOfArrayList(nRows) /* key set */ + nRows * (
                SizedUtil.MAP_ENTRY_SIZE + /* entry set */
                keySize + // key size
                valueSize); // value size
    }
}
