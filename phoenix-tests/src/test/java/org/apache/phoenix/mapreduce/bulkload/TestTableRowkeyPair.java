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
package org.apache.phoenix.mapreduce.bulkload;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@linkplain TableRowkeyPair} 
 */
public class TestTableRowkeyPair {

    @Test
    public void testRowkeyPair() throws IOException {
        testsRowsKeys("first", "aa", "first", "aa", 0);
        testsRowsKeys("first", "aa", "first", "ab", -1);
        testsRowsKeys("second", "aa", "first", "aa", 1);
        testsRowsKeys("first", "aa", "first", "aaa", -1);
        testsRowsKeys("first","bb", "first", "aaaa", 1);
    }

    private void testsRowsKeys(String aTable, String akey, String bTable, String bkey, int expectedSignum) throws IOException {
        
        final ImmutableBytesWritable arowkey = new ImmutableBytesWritable(Bytes.toBytes(akey));
        TableRowkeyPair pair1 = new TableRowkeyPair(aTable, arowkey);
        
        ImmutableBytesWritable browkey = new ImmutableBytesWritable(Bytes.toBytes(bkey));
        TableRowkeyPair pair2 = new TableRowkeyPair(bTable, browkey);
        
        TableRowkeyPair.Comparator comparator = new TableRowkeyPair.Comparator();
        try( ByteArrayOutputStream baosA = new ByteArrayOutputStream();
             ByteArrayOutputStream baosB = new ByteArrayOutputStream()) {
            
            pair1.write(new DataOutputStream(baosA));
            pair2.write(new DataOutputStream(baosB));
            Assert.assertEquals(expectedSignum , signum(pair1.compareTo(pair2)));
            Assert.assertEquals(expectedSignum , signum(comparator.compare(baosA.toByteArray(), 0, baosA.size(), baosB.toByteArray(), 0, baosB.size())));
            Assert.assertEquals(expectedSignum, -signum(comparator.compare(baosB.toByteArray(), 0, baosB.size(), baosA.toByteArray(), 0, baosA.size())));
        }

    }
    
    private int signum(int i) {
        return i > 0 ? 1: (i == 0 ? 0: -1);
    }
}
