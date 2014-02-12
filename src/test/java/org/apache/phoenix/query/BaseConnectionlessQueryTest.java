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
package org.apache.phoenix.query;

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.FUNKY_NAME;
import static org.apache.phoenix.util.TestUtil.MULTI_CF_NAME;
import static org.apache.phoenix.util.TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;

import java.sql.DriverManager;
import java.util.Properties;

import org.junit.BeforeClass;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;



public class BaseConnectionlessQueryTest extends BaseTest {

    public static PTable ATABLE;
    public static PColumn ORGANIZATION_ID;
    public static PColumn ENTITY_ID;
    public static PColumn A_INTEGER;
    public static PColumn A_STRING;
    public static PColumn B_STRING;
    public static PColumn A_DATE;
    public static PColumn A_TIME;
    public static PColumn A_TIMESTAMP;
    public static PColumn X_DECIMAL;
    
    protected static String getUrl() {
        return TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        startServer(getUrl());
        ensureTableCreated(getUrl(), ATABLE_NAME);
        ensureTableCreated(getUrl(), ENTITY_HISTORY_TABLE_NAME);
        ensureTableCreated(getUrl(), FUNKY_NAME);
        ensureTableCreated(getUrl(), PTSDB_NAME);
        ensureTableCreated(getUrl(), MULTI_CF_NAME);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_TABLE_TIMESTAMP));
        PhoenixConnection conn = DriverManager.getConnection(PHOENIX_CONNECTIONLESS_JDBC_URL, props).unwrap(PhoenixConnection.class);
        try {
            PTable table = conn.getPMetaData().getTable(ATABLE_NAME);
            ATABLE = table;
            ORGANIZATION_ID = table.getColumn("ORGANIZATION_ID");
            ENTITY_ID = table.getColumn("ENTITY_ID");
            A_INTEGER = table.getColumn("A_INTEGER");
            A_STRING = table.getColumn("A_STRING");
            B_STRING = table.getColumn("B_STRING");
            ENTITY_ID = table.getColumn("ENTITY_ID");
            A_DATE = table.getColumn("A_DATE");
            A_TIME = table.getColumn("A_TIME");
            A_TIMESTAMP = table.getColumn("A_TIMESTAMP");
            X_DECIMAL = table.getColumn("X_DECIMAL");
        } finally {
            conn.close();
        }
    }
    

}
