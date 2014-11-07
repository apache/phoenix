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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.junit.Test;

public class PhoenixResultSetMetadataTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testColumnDisplaySize() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T (pk1 CHAR(15) not null, pk2 VARCHAR not null,  v1 VARCHAR(15), v2 DATE, v3 VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        ResultSet rs = conn.createStatement().executeQuery("SELECT pk1, pk2, v1, v2, NULL FROM T");
        assertEquals(15, rs.getMetaData().getColumnDisplaySize(1));
        assertEquals(PhoenixResultSetMetaData.DEFAULT_DISPLAY_WIDTH, rs.getMetaData().getColumnDisplaySize(2));
        assertEquals(15, rs.getMetaData().getColumnDisplaySize(3));
        assertEquals(conn.unwrap(PhoenixConnection.class).getDatePattern().length(), rs.getMetaData().getColumnDisplaySize(4));
        assertEquals(QueryConstants.NULL_DISPLAY_TEXT.length(), rs.getMetaData().getColumnDisplaySize(5));
    }
}
