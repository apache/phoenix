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

package org.apache.phoenix.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.hbase.index.ValueGetter;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import com.google.common.collect.Maps;

public class IndexMaintainerTest  extends BaseConnectionlessQueryTest {
    private static final String DEFAULT_SCHEMA_NAME = "";
    private static final String DEFAULT_TABLE_NAME = "rkTest";
    
    private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns, Object[] values) throws Exception {
        testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns, values, "", "", "");
    }

    private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns, Object[] values, String includeColumns) throws Exception {
        testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns, values, includeColumns, "", "");
    }

    private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns, Object[] values, String includeColumns, String dataProps, String indexProps) throws Exception {
        testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns, values, "", dataProps, indexProps);
    }

    private static ValueGetter newValueGetter(final byte[] row, final Map<ColumnReference, byte[]> valueMap) {
        return new ValueGetter() {

            @Override
            public ImmutableBytesPtr getLatestValue(ColumnReference ref) {
                return new ImmutableBytesPtr(valueMap.get(ref));
            }

			@Override
			public byte[] getRowKey() {
				return row;
			}
            
        };
    }
    
    private void testIndexRowKeyBuilding(String schemaName, String tableName, String dataColumns, String pk, String indexColumns, Object[] values, String includeColumns, String dataProps, String indexProps) throws Exception {
        KeyValueBuilder builder = GenericKeyValueBuilder.INSTANCE;
        testIndexRowKeyBuilding(schemaName, tableName, dataColumns, pk, indexColumns, values, includeColumns, dataProps, indexProps, builder);
    }

    private void testIndexRowKeyBuilding(String schemaName, String tableName, String dataColumns,
            String pk, String indexColumns, Object[] values, String includeColumns,
            String dataProps, String indexProps, KeyValueBuilder builder) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),SchemaUtil.normalizeIdentifier(tableName));
        String fullIndexName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),SchemaUtil.normalizeIdentifier("idx"));
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(" + dataColumns + " CONSTRAINT pk PRIMARY KEY (" + pk + "))  " + (dataProps.isEmpty() ? "" : dataProps) );
        try {
            conn.createStatement().execute("CREATE INDEX idx ON " + fullTableName + "(" + indexColumns + ") " + (includeColumns.isEmpty() ? "" : "INCLUDE (" + includeColumns + ") ") + (indexProps.isEmpty() ? "" : indexProps));
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable table = pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName));
            PTable index = pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(),fullIndexName));
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            table.getIndexMaintainers(ptr, pconn);
            List<IndexMaintainer> c1 = IndexMaintainer.deserialize(ptr, builder);
            assertEquals(1,c1.size());
            IndexMaintainer im1 = c1.get(0);
            
            StringBuilder buf = new StringBuilder("UPSERT INTO " + fullTableName  + " VALUES(");
            for (int i = 0; i < values.length; i++) {
                buf.append("?,");
            }
            buf.setCharAt(buf.length()-1, ')');
            PreparedStatement stmt = conn.prepareStatement(buf.toString());
            for (int i = 0; i < values.length; i++) {
                stmt.setObject(i+1, values[i]);
            }
            stmt.execute();
            	Iterator<Pair<byte[],List<KeyValue>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
            List<KeyValue> dataKeyValues = iterator.next().getSecond();
            Map<ColumnReference,byte[]> valueMap = Maps.newHashMapWithExpectedSize(dataKeyValues.size());
            byte[] row = dataKeyValues.get(0).getRow();
			ImmutableBytesWritable rowKeyPtr = new ImmutableBytesWritable(row);
            Put dataMutation = new Put(rowKeyPtr.copyBytes());
            for (KeyValue kv : dataKeyValues) {
                valueMap.put(new ColumnReference(kv.getFamily(),kv.getQualifier()), kv.getValue());
                dataMutation.add(kv);
            }
            ValueGetter valueGetter = newValueGetter(row, valueMap);
            
            List<Mutation> indexMutations =
                    IndexTestUtil.generateIndexData(index, table, dataMutation, ptr, builder);
            assertEquals(1,indexMutations.size());
            assertTrue(indexMutations.get(0) instanceof Put);
            Mutation indexMutation = indexMutations.get(0);
            ImmutableBytesWritable indexKeyPtr = new ImmutableBytesWritable(indexMutation.getRow());
            ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
            byte[] mutablelndexRowKey = im1.buildRowKey(valueGetter, ptr, null, null);
            byte[] immutableIndexRowKey = indexKeyPtr.copyBytes();
            assertArrayEquals(immutableIndexRowKey, mutablelndexRowKey);
            for (ColumnReference ref : im1.getCoverededColumns()) {
                valueMap.get(ref);
            }
            byte[] dataRowKey = im1.buildDataRowKey(indexKeyPtr, null);
            assertArrayEquals(dataRowKey, dataKeyValues.get(0).getRow());
        } finally {
            try {
                conn.createStatement().execute("DROP TABLE " + fullTableName);
            } finally {
                conn.close();
            }
        }
    }

    @Test
    public void testRowKeyVarOnlyIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL", "k1,k2", "k2, k1", new Object [] {"a",1.1});
    }
 
    @Test
    public void testVarFixedndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2, k1", new Object [] {"a",1.1});
    }
 
    
    @Test
    public void testCompositeRowKeyVarFixedIndex() throws Exception {
        // TODO: using 1.1 for INTEGER didn't give error
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2, k1", new Object [] {"a",1});
    }
 
    @Test
    public void testCompositeRowKeyVarFixedAtEndIndex() throws Exception {
        // Forces trailing zero in index key for fixed length
        for (int i = 0; i < 10; i++) {
            testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, k3 VARCHAR, v VARCHAR", "k1,k2,k3", "k1, k3, k2", new Object [] {"a",i, "b"});
        }
    }
 
   @Test
    public void testSingleKeyValueIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER, v VARCHAR", "k1", "v", new Object [] {"a",1,"b"});
    }
 
    @Test
    public void testMultiKeyValueIndex() throws Exception {
        testIndexRowKeyBuilding("k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT", "k1, k2", "v2, k2, v1", new Object [] {"a",1,2.2,"bb"});
    }
 
    @Test
    public void testMultiKeyValueCoveredIndex() throws Exception {
        testIndexRowKeyBuilding("k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2", "v2, k2, v1", new Object [] {"a",1,2.2,"bb"}, "v3, v4");
    }
 
    @Test
    public void testSingleKeyValueDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER, v VARCHAR", "k1", "v DESC", new Object [] {"a",1,"b"});
    }
 
    @Test
    public void testCompositeRowKeyVarFixedDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2 DESC, k1", new Object [] {"a",1});
    }
 
    @Test
    public void testCompositeRowKeyTimeIndex() throws Exception {
        long timeInMillis = System.currentTimeMillis();
        long timeInNanos = System.nanoTime();
        Timestamp ts = new Timestamp(timeInMillis);
        ts.setNanos((int) (timeInNanos % 1000000000));
        testIndexRowKeyBuilding("ts1 DATE NOT NULL, ts2 TIME NOT NULL, ts3 TIMESTAMP NOT NULL", "ts1,ts2,ts3", "ts2, ts1", new Object [] {new Date(timeInMillis), new Time(timeInMillis), ts});
    }
 
    @Test
    public void testCompositeRowKeyBytesIndex() throws Exception {
        long timeInMillis = System.currentTimeMillis();
        long timeInNanos = System.nanoTime();
        Timestamp ts = new Timestamp(timeInMillis);
        ts.setNanos((int) (timeInNanos % 1000000000));
        testIndexRowKeyBuilding("b1 BINARY(3) NOT NULL, v VARCHAR", "b1,v", "v, b1", new Object [] {new byte[] {41,42,43}, "foo"});
    }
 
    @Test
    public void testCompositeDescRowKeyVarFixedDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1});
    }
 
    @Test
    public void testCompositeDescRowKeyVarDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1.1,"b"});
    }
 
    @Test
    public void testCompositeDescRowKeyVarAscIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL NOT NULL, v VARCHAR", "k1, k2 DESC", "k2, k1", new Object [] {"a",1.1,"b"});
    }
 
    @Test
    public void testCompositeDescRowKeyVarFixedDescSaltedIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1}, "", "", "SALT_BUCKETS=4");
    }
 
    @Test
    public void testCompositeDescRowKeyVarFixedDescSaltedIndexSaltedTable() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1}, "", "SALT_BUCKETS=3", "SALT_BUCKETS=3");
    }
 
    @Test
    public void testMultiKeyValueCoveredSaltedIndex() throws Exception {
        testIndexRowKeyBuilding("k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2", "v2 DESC, k2 DESC, v1", new Object [] {"a",1,2.2,"bb"}, "v3, v4", "", "SALT_BUCKETS=4");
    }
 
    @Test
    public void tesIndexWithBigInt() throws Exception {
        testIndexRowKeyBuilding(
                "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BIGINT, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2",
                "v1 DESC, k2 DESC", new Object[] { "a", 1, 2.2, "bb" });
    }
    
    @Test
    public void tesIndexWithAscBoolean() throws Exception {
        testIndexRowKeyBuilding(
                "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2",
                "v1, k2 DESC", new Object[] { "a", 1, true, "bb" });
    }
    
    @Test
    public void tesIndexWithAscNullBoolean() throws Exception {
        testIndexRowKeyBuilding(
                "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2",
                "v1, k2 DESC", new Object[] { "a", 1, null, "bb" });
    }
    
    @Test
    public void tesIndexWithAscFalseBoolean() throws Exception {
        testIndexRowKeyBuilding(
                "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2",
                "v1, k2 DESC", new Object[] { "a", 1, false, "bb" });
    }
    
    @Test
    public void tesIndexWithDescBoolean() throws Exception {
        testIndexRowKeyBuilding(
                "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2",
                "v1 DESC, k2 DESC", new Object[] { "a", 1, true, "bb" });
    }
    
    @Test
    public void tesIndexWithDescFalseBoolean() throws Exception {
        testIndexRowKeyBuilding(
                "k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 BOOLEAN, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2",
                "v1 DESC, k2 DESC", new Object[] { "a", 1, false, "bb" });
    }
}
