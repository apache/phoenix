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
package org.apache.phoenix.schema.tuple;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleKeyValueTupleTest {

    @Test
    public void testToString() {

        SingleKeyValueTuple singleKeyValueTuple = new SingleKeyValueTuple();
        assertTrue(singleKeyValueTuple.toString().equals("SingleKeyValueTuple[null]"));
        final byte [] rowKey = Bytes.toBytes("aaa");
        singleKeyValueTuple.setKey(new ImmutableBytesWritable(rowKey));
        assertTrue(singleKeyValueTuple.toString().equals("SingleKeyValueTuple[aaa]"));

        byte [] family1 = Bytes.toBytes("abc");
        byte [] qualifier1 = Bytes.toBytes("def");
        KeyValue keyValue = new KeyValue(rowKey, family1, qualifier1, 0L, Type.Put, rowKey);
        singleKeyValueTuple = new SingleKeyValueTuple(keyValue);
        assertTrue(singleKeyValueTuple.toString().startsWith("SingleKeyValueTuple[aaa/abc:def/0/Put/vlen=3"));
        assertTrue(singleKeyValueTuple.toString().endsWith("]"));
    }

}
