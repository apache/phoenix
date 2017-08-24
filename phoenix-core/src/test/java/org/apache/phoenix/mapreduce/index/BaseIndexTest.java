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
package org.apache.phoenix.mapreduce.index;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * Creates a simple data table and index table
 *
 */
public class BaseIndexTest extends BaseConnectionlessQueryTest {
    protected static final String SCHEMA_NAME = "TEST_SCHEMA";
    protected static final String DATA_TABLE_NAME = "TEST_INDEX_COLUMN_NAMES_UTIL";
    protected static final String INDEX_TABLE_NAME = "TEST_ICN_INDEX";
    protected static final String DATA_TABLE_FULL_NAME = SCHEMA_NAME + "." + DATA_TABLE_NAME;
    protected static final String INDEX_TABLE_FULL_NAME = SCHEMA_NAME + "." + INDEX_TABLE_NAME;

    private static final String DATA_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS " + DATA_TABLE_FULL_NAME + "\n" +
            "(\n" +
            "    ID INTEGER NOT NULL,\n" +
            "    PK_PART2 TINYINT NOT NULL,\n" +
            "    NAME VARCHAR,\n" +
            "    ZIP BIGINT,\n" +
            "    EMPLOYER CHAR(20),\n" +
            "    CONSTRAINT PK PRIMARY KEY\n" +
            "    (\n" +
            "        ID,\n" +
            "        PK_PART2\n" +
            "        \n" +
            "    )\n" +
            ")";

    private static final String INDEX_TABLE_DDL =
            "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME
                    + " (NAME) INCLUDE (ZIP)";
    protected PTable pDataTable;
    protected PTable pIndexTable;
    protected Connection conn;

    @BeforeClass
    public static void setupClass() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().execute(DATA_TABLE_DDL);
            conn.createStatement().execute(INDEX_TABLE_DDL);
        } finally {
            conn.close();
        }
    }

    @Before
    public void setup() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        pDataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), DATA_TABLE_FULL_NAME));
        pIndexTable = pconn.getTable(new PTableKey(pconn.getTenantId(), INDEX_TABLE_FULL_NAME));
    }

    @After
    public void tearDown() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
