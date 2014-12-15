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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RowKeySchemaTest  extends BaseConnectionlessQueryTest  {

    public RowKeySchemaTest() {
    }

    private void assertExpectedRowKeyValue(String dataColumns, String pk, Object[] values) throws Exception {
        assertIteration(dataColumns,pk,values,"");
    }
    
    private void assertIteration(String dataColumns, String pk, Object[] values, String dataProps) throws Exception {
        String schemaName = "";
        String tableName = "T";
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),SchemaUtil.normalizeIdentifier(tableName));
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(" + dataColumns + " CONSTRAINT pk PRIMARY KEY (" + pk + "))  " + (dataProps.isEmpty() ? "" : dataProps) );
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName));
        conn.close();
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
        KeyValue keyValue = dataKeyValues.get(0);
        
        List<SortOrder> sortOrders = Lists.newArrayListWithExpectedSize(table.getPKColumns().size());
        for (PColumn col : table.getPKColumns()) {
            sortOrders.add(col.getSortOrder());
        }
        RowKeySchema schema = table.getRowKeySchema();
        int minOffset = keyValue.getRowOffset();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        int nExpectedValues = values.length;
        for (int i = values.length-1; i >=0; i--) {
            if (values[i] == null) {
                nExpectedValues--;
            } else {
                break;
            }
        }
        int i = 0;
        int maxOffset = schema.iterator(keyValue.getRowArray(), minOffset, keyValue.getRowLength(), ptr);
        for (i = 0; i < schema.getFieldCount(); i++) {
            Boolean hasValue = schema.next(ptr, i, maxOffset);
            if (hasValue == null) {
                break;
            }
            assertTrue(hasValue);
            PDataType type = PDataType.fromLiteral(values[i]);
            SortOrder sortOrder = sortOrders.get(i);
            Object value = type.toObject(ptr, schema.getField(i).getDataType(), sortOrder);
            assertEquals(values[i], value);
        }
        assertEquals(nExpectedValues, i);
        assertNull(schema.next(ptr, i, maxOffset));
        
        for (i--; i >= 0; i--) {
            Boolean hasValue = schema.previous(ptr, i, minOffset);
            if (hasValue == null) {
                break;
            }
            assertTrue(hasValue);
            PDataType type = PDataType.fromLiteral(values[i]);
            SortOrder sortOrder = sortOrders.get(i);
            Object value = type.toObject(ptr, schema.getField(i).getDataType(), sortOrder);
            assertEquals(values[i], value);
        }
        assertEquals(-1, i);
        assertNull(schema.previous(ptr, i, minOffset));
     }
    
    @Test
    public void testFixedLengthValueAtEnd() throws Exception {
        assertExpectedRowKeyValue("n VARCHAR NOT NULL, s CHAR(1) NOT NULL, y SMALLINT NOT NULL, o BIGINT NOT NULL", "n,s,y DESC,o DESC", new Object[] {"Abbey","F",2012,253});
    }
    
    @Test
    public void testFixedVarVar() throws Exception {
        assertExpectedRowKeyValue("i INTEGER NOT NULL, v1 VARCHAR, v2 VARCHAR", "i, v1, v2", new Object[] {1, "a", "b"});
    }
    
    @Test
    public void testFixedFixedVar() throws Exception {
        assertExpectedRowKeyValue("c1 INTEGER NOT NULL, c2 BIGINT NOT NULL, c3 VARCHAR", "c1, c2, c3", new Object[] {1, 2, "abc"});
    }
    
    @Test
    public void testVarNullNull() throws Exception {
        assertExpectedRowKeyValue("c1 VARCHAR, c2 VARCHAR, c3 VARCHAR", "c1, c2, c3", new Object[] {"abc", null, null});
    }

    @Test
    public void testVarFixedVar() throws Exception {
        assertExpectedRowKeyValue("c1 VARCHAR, c2 CHAR(1) NOT NULL, c3 VARCHAR", "c1, c2, c3", new Object[] {"abc", "z", "de"});
    }
    
    @Test
    public void testVarFixedFixed() throws Exception {
        assertExpectedRowKeyValue("c1 VARCHAR, c2 CHAR(1) NOT NULL, c3 INTEGER NOT NULL", "c1, c2, c3", new Object[] {"abc", "z", 5});
    }
    
}
