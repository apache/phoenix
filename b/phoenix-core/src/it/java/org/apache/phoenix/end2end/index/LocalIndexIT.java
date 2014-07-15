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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableKey;
import org.junit.Test;

public class LocalIndexIT extends BaseIndexIT {
    private void createBaseTable(String tableName, Integer saltBuckets) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                + (saltBuckets == null ? "" : (",salt_buckets="+saltBuckets));
        conn.createStatement().execute(ddl);
        conn.close();
    }
    
    @Test
    public void testLocalIndexRoundTrip() throws Exception {
        createBaseTable(DATA_TABLE_NAME, null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        Connection conn2 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("CREATE LOCAL INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + "(v1)");
        conn2.createStatement().executeQuery("SELECT * FROM " + DATA_TABLE_FULL_NAME).next();
        PTable localIndex = conn2.unwrap(PhoenixConnection.class).getMetaDataCache().getTable(new PTableKey(null,INDEX_TABLE_NAME));
        assertEquals(IndexType.LOCAL, localIndex.getIndexType());
        assertNotNull(localIndex.getViewIndexId());
    }

}
