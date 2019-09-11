/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.apache.phoenix.hbase.index.wal.KeyValueCodec;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IndexedKeyValueTest {

    private static final byte[] ROW_KEY = Bytes.toBytes("foo");
    private static final byte[] FAMILY = Bytes.toBytes("family");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
    private static final byte[] VALUE = Bytes.toBytes("value");
    private static final byte[] TABLE_NAME = Bytes.toBytes("MyTableName");

    @Test
    public void testIndexedKeyValueExceptionWhenMutationEmpty() throws IOException {
        boolean caughtNullMutation = false, caughtNullEntry = false;
        try {
            IndexedKeyValue ikv = IndexedKeyValue.newIndexedKeyValue(TABLE_NAME, null);
        } catch (IllegalArgumentException iae){
            caughtNullMutation = true;
        }
        try {
            Mutation m = new Put(ROW_KEY);
            IndexedKeyValue ikv = IndexedKeyValue.newIndexedKeyValue(TABLE_NAME, m);
        } catch (IllegalArgumentException iae){
            caughtNullEntry = true;
        }
        //no need to test adding a mutation with a Cell with just a row key; HBase will put in
        //a default cell with family byte[0], qualifier and value of "", and LATEST_TIMESTAMP

        Assert.assertTrue(caughtNullMutation & caughtNullEntry);

    }

    @Test
    public void testIndexedKeyValuePopulatesKVFields() throws Exception {
        byte[] row = (ROW_KEY);
        Put mutation = new Put(row);
        mutation.addColumn(FAMILY, QUALIFIER, VALUE);
        IndexedKeyValue indexedKeyValue = IndexedKeyValue.newIndexedKeyValue(TABLE_NAME, mutation);
        testIndexedKeyValueHelper(indexedKeyValue, row, TABLE_NAME, mutation);

        //now serialize the IndexedKeyValue and make sure the deserialized copy also
        //has all the right fields
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        KeyValueCodec.write(out, indexedKeyValue);

        IndexedKeyValue deSerializedKV = (IndexedKeyValue)
            KeyValueCodec.readKeyValue(new DataInputStream(
                new ByteArrayInputStream(baos.toByteArray())));
        testIndexedKeyValueHelper(deSerializedKV, row, TABLE_NAME, mutation);

    }

    private void testIndexedKeyValueHelper(IndexedKeyValue indexedKeyValue, byte[] row,
                                           byte[] tableNameBytes, Mutation mutation) {
        Assert.assertArrayEquals(row, CellUtil.cloneRow(indexedKeyValue));
        Assert.assertArrayEquals(tableNameBytes, indexedKeyValue.getIndexTable());
        Assert.assertEquals(mutation.toString(), indexedKeyValue.getMutation().toString());
        Assert.assertArrayEquals(WALEdit.METAFAMILY, CellUtil.cloneFamily(indexedKeyValue));
    }

}
