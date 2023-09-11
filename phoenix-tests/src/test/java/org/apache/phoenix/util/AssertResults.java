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

import static org.junit.Assert.assertTrue;

import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Utility class to assert that a scan returns the expected results
 *
 * 
 * @since 0.1
 */
public class AssertResults {
    public static final AssertingIterator NONE = new NoopAssertingIterator();
    
    private AssertResults() {    
    }
    
    public static void assertResults(ResultIterator scanner, Tuple[] results) throws Exception {
        assertResults(scanner,new ResultAssertingIterator(Arrays.asList(results).iterator()));
    }
    
    public static void assertUnorderedResults(ResultIterator scanner, Tuple[] results) throws Exception {
        assertResults(scanner,new UnorderedResultAssertingIterator(Arrays.asList(results)));
    }
    
    public static void assertResults(ResultIterator scanner, AssertingIterator iterator) throws Exception {
        try {
            for (Tuple result = scanner.next(); result != null; result = scanner.next()) {
                iterator.assertNext(result);
            }
            iterator.assertDone();
        } finally {
            scanner.close();
        }
    }
    
    public static interface AssertingIterator {
        public void assertNext(Tuple result) throws Exception;
        public void assertDone() throws Exception;
    }
    
    /**
     * 
     * Use to iterate through results without checking the values against 
     *
     * 
     * @since 0.1
     */
    private static final class NoopAssertingIterator implements AssertingIterator {
        @Override
        public void assertDone() throws Exception {
        }

        @Override
        public void assertNext(Tuple result) throws Exception {
        }
    }
    
    public static class ResultAssertingIterator implements AssertingIterator {
        private final Iterator<Tuple> expectedResults;
        
        public ResultAssertingIterator(Iterator<Tuple> expectedResults) {
            this.expectedResults = expectedResults;
        }
        
        @Override
        public void assertDone() {
            assertTrue(!expectedResults.hasNext());
        }

        @Override
        public void assertNext(Tuple result) throws Exception {
            assertTrue(expectedResults.hasNext());
            Tuple expected = expectedResults.next();
            TestUtil.compareTuples(expected, result);
        }
    }

    public static class UnorderedResultAssertingIterator implements AssertingIterator {
        private final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
        private final Map<ImmutableBytesWritable, Tuple> expectedResults;
        
        public UnorderedResultAssertingIterator(Collection<Tuple> expectedResults) {
            this.expectedResults = new HashMap<ImmutableBytesWritable,Tuple>(expectedResults.size());
            for (Tuple result : expectedResults) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                result.getKey(ptr);
                this.expectedResults.put(ptr,result);
            }
        }
        
        @Override
        public void assertDone() {
            assertTrue(expectedResults.isEmpty());
        }

        @Override
        public void assertNext(Tuple result) throws Exception {
            result.getKey(tempPtr);
            Tuple expected = expectedResults.remove(tempPtr);
            TestUtil.compareTuples(expected, result);
        }
    }
    
}
