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
package org.apache.phoenix.iterate;

import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.memory.DelegatingMemoryManager;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.AssertResults;
import org.junit.Test;



public class SpoolingResultIteratorTest {
    private final static byte[] A = Bytes.toBytes("a");
    private final static byte[] B = Bytes.toBytes("b");

    private void testSpooling(int threshold, long maxSizeSpool) throws Throwable {
        Tuple[] results = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };
        PeekingResultIterator iterator = new MaterializedResultIterator(Arrays.asList(results));

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };

        MemoryManager memoryManager = new DelegatingMemoryManager(new GlobalMemoryManager(threshold, 0));
        ResultIterator scanner = new SpoolingResultIterator(iterator, memoryManager, threshold, maxSizeSpool,"/tmp");
        AssertResults.assertResults(scanner, expectedResults);
    }

    @Test
    public void testInMemorySpooling() throws Throwable {
        testSpooling(1024*1024, QueryServicesOptions.DEFAULT_MAX_SPOOL_TO_DISK_BYTES);
    }
    @Test
    public void testOnDiskSpooling() throws Throwable {
        testSpooling(1, QueryServicesOptions.DEFAULT_MAX_SPOOL_TO_DISK_BYTES);
    }

    @Test(expected = SpoolTooBigToDiskException.class)
    public void testFailToSpool() throws Throwable{
    		testSpooling(1, 0L);
    }
}
