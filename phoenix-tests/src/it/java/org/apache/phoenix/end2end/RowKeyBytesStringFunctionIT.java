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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class RowKeyBytesStringFunctionIT extends ParallelStatsDisabledIT {

    @Test
    public void getRowKeyBytesAndVerify() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            int[] values = {3,7,9,158,5};
            String tableName = generateUniqueName();
            String ddl =
                    "CREATE TABLE IF NOT EXISTS " + tableName + " "
                            + "(id INTEGER NOT NULL, pkcol VARCHAR, page_id UNSIGNED_LONG,"
                            + " \"DATE\" BIGINT, \"value\" INTEGER,"
                            + " constraint pk primary key(id, pkcol)) COLUMN_ENCODED_BYTES = 0";
            conn.createStatement().execute(ddl);

            conn.createStatement().execute("UPSERT INTO " + tableName
                    + " (id, pkcol, page_id, \"DATE\", \"value\") VALUES (1, 'a', 8, 1," + values[0] + ")");
            conn.createStatement().execute("UPSERT INTO " + tableName
                    + " (id, pkcol, page_id, \"DATE\", \"value\") VALUES (2, 'ab', 8, 2," + values[1] + ")");
            conn.createStatement().execute("UPSERT INTO " + tableName
                    + " (id, pkcol, page_id, \"DATE\", \"value\") VALUES (3, 'abc', 8, 3," + values[2] + ")");
            conn.createStatement().execute("UPSERT INTO " + tableName
                    + " (id, pkcol, page_id, \"DATE\", \"value\") VALUES (5, 'abcde', 8, 5," + values[4] + ")");
            conn.createStatement().execute("UPSERT INTO " + tableName
                    + " (id, pkcol, page_id, \"DATE\", \"value\") VALUES (4, 'abcd', 8, 4," + values[3] + ")");
            conn.commit();

            ResultSet rs =
                    conn.createStatement().executeQuery("SELECT ROWKEY_BYTES_STRING() FROM " + tableName);
            try (org.apache.hadoop.hbase.client.Connection hconn =
                    ConnectionFactory.createConnection(config)) {
                Table table = hconn.getTable(TableName.valueOf(tableName));
                int i = 0;
                while (rs.next()) {
                    String s = rs.getString(1);
                    Get get = new Get(Bytes.toBytesBinary(s));
                    Result hbaseRes = table.get(get);
                    assertFalse(hbaseRes.isEmpty());
                    assertTrue(Bytes.equals(hbaseRes.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes("value")), 
                        PInteger.INSTANCE.toBytes(values[i])));
                    i++;
                }
            }
        }
    }
}
