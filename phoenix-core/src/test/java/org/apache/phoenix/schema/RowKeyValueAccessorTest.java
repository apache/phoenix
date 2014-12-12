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

public class RowKeyValueAccessorTest  extends BaseConnectionlessQueryTest  {

    public RowKeyValueAccessorTest() {
    }

    private void assertExpectedRowKeyValue(String dataColumns, String pk, Object[] values, int index) throws Exception {
        assertExpectedRowKeyValue(dataColumns,pk,values,index,"");
    }
    
    private void assertExpectedRowKeyValue(String dataColumns, String pk, Object[] values, int index, String dataProps) throws Exception {
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
        
        List<PColumn> pkColumns = table.getPKColumns();
        RowKeyValueAccessor accessor = new RowKeyValueAccessor(pkColumns, 3);
        int offset = accessor.getOffset(keyValue.getRowArray(), keyValue.getRowOffset());
        int length = accessor.getLength(keyValue.getRowArray(), offset, keyValue.getOffset()+keyValue.getLength());
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(keyValue.getRowArray(), offset, length);
        
        PDataType dataType = pkColumns.get(index).getDataType();
        Object expectedObject = dataType.toObject(values[index], PDataType.fromLiteral(values[index]));
        dataType.coerceBytes(ptr, dataType, pkColumns.get(index).getSortOrder(), SortOrder.getDefault());
        Object actualObject = dataType.toObject(ptr);
        assertEquals(expectedObject, actualObject);
    }
    
    @Test
    public void testFixedLengthValueAtEnd() throws Exception {
        assertExpectedRowKeyValue("n VARCHAR NOT NULL, s CHAR(1) NOT NULL, y SMALLINT NOT NULL, o BIGINT NOT NULL", "n,s,y DESC,o DESC", new Object[] {"Abbey","F",2012,253}, 3);
    }
}
