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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;

/**
 * 
 * Utilities for KeyValue. Where there's duplication with KeyValue methods,
 * these avoid creating new objects when not necessary (primary preventing
 * byte array copying).
 *
 * 
 * @since 0.1
 */
public class KeyValueUtil {
    private KeyValueUtil() {
    }

    public static KeyValue newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key, 0, key.length,
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(ImmutableBytesWritable key, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key.get(), key.getOffset(), key.getLength(),
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, byte[] cq, long ts, byte[] value, int valueOffset, int valueLength) {
        return new KeyValue(key, keyOffset, keyLength,
                cf, 0, cf.length,
                cq, 0, cq.length,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }

    public static KeyValue newKeyValue(byte[] key, int keyOffset, int keyLength, byte[] cf, 
        int cfOffset, int cfLength, byte[] cq, int cqOffset, int cqLength, long ts, byte[] value, 
        int valueOffset, int valueLength) {
        return new KeyValue(key, keyOffset, keyLength,
                cf, cfOffset, cfLength,
                cq, cqOffset, cqLength,
                ts, Type.Put,
                value, valueOffset, valueLength);
    }
    
    public static KeyValue newKeyValue(byte[] key, byte[] cf, byte[] cq, long ts, byte[] value) {
        return newKeyValue(key,cf,cq,ts,value,0,value.length);
    }

    /**
     * Binary search for latest column value without allocating memory in the process
     * @param kvBuilder TODO
     * @param kvs
     * @param family
     * @param qualifier
     */
    public static KeyValue getColumnLatest(KeyValueBuilder kvBuilder, List<KeyValue>kvs, byte[] family, byte[] qualifier) {
        if (kvs.size() == 0) {
        	return null;
        }
        KeyValue kvForRow = kvs.get(0);
        Comparator<KeyValue> comp = new SearchComparator(kvBuilder, kvForRow, family, qualifier);
        // pos === ( -(insertion point) - 1)
        int pos = Collections.binarySearch(kvs, null, comp);
        // never will exact match
        if (pos < 0) {
          pos = (pos+1) * -1;
          // pos is now insertion point
        }
        if (pos == kvs.size()) {
          return null; // doesn't exist
        }
    
        KeyValue kv = kvs.get(pos);
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
                family, 0, family.length) != 0) {
            return null;
        }
        if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
                qualifier, 0, qualifier.length) != 0) {
            return null;
        }
        return kv;
    }


    /**
     * Binary search for latest column value without allocating memory in the process
     * @param kvBuilder TODO
     * @param kvs
     * @param family
     * @param qualifier
     */
    public static KeyValue getColumnLatest(KeyValueBuilder kvBuilder, KeyValue[] kvs, byte[] family, byte[] qualifier) {
        if (kvs.length == 0) {
            return null;
        }
        KeyValue kvForRow = kvs[0];
        Comparator<KeyValue> comp = new SearchComparator(kvBuilder, kvForRow, family, qualifier);
        // pos === ( -(insertion point) - 1)
        int pos = Arrays.binarySearch(kvs, null, comp);
        // never will exact match
        if (pos < 0) {
          pos = (pos+1) * -1;
          // pos is now insertion point
        }
        if (pos == kvs.length) {
          return null; // doesn't exist
        }
    
        KeyValue kv = kvs[pos];
        if (Bytes.compareTo(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
                family, 0, family.length) != 0) {
            return null;
        }
        if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
                qualifier, 0, qualifier.length) != 0) {
            return null;
        }
        return kv;
    }

    /*
     * Special comparator, *only* works for binary search.
     * Current JDKs only uses the search term on the right side,
     * Making use of that saves instanceof checks, and allows us
     * to inline the search term in the comparator
     */
	private static class SearchComparator implements Comparator<KeyValue> {
	    private final KeyValueBuilder kvBuilder;
		private final KeyValue r;
		private final byte[] family;
		private final byte[] qualifier;

		public SearchComparator(KeyValueBuilder kvBuilder, KeyValue r, byte[] f, byte[] q) {
		    this.kvBuilder = kvBuilder;
			this.r = r;
			family = f;
			qualifier = q;
		}

		@Override
        public int compare(final KeyValue l, final KeyValue ignored) {
			assert ignored == null;
            KVComparator comparator = kvBuilder.getKeyValueComparator();
			// row
			int val = comparator.compareRows(l, r);
			if (val != 0) {
				return val;
			}
            // family
			val = kvBuilder.compareFamily(l, family, 0, family.length);
			if (val != 0) {
				return val;
			}
			// qualifier
            val = kvBuilder.compareQualifier(l, qualifier, 0, qualifier.length);
			if (val != 0) {
				return val;
			}
			// We want the latest TS and type, so we get the first one.
			// This assumes they KV are passed in ordered from latest to earliest,
			// as that's the order the server sends them.
			return 1;
		}
	}
}
