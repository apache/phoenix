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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.FUNKY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.MULTI_CF_NAME;
import static org.apache.phoenix.util.TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
import static org.apache.phoenix.util.TestUtil.PTSDB2_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB3_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;



public class BaseConnectionlessQueryTest extends BaseTest {

    public static PTable ATABLE;
    public static Expression ORGANIZATION_ID;
    public static Expression ENTITY_ID;
    public static Expression A_INTEGER;
    public static Expression A_STRING;
    public static Expression B_STRING;
    public static Expression A_DATE;
    public static Expression A_TIME;
    public static Expression A_TIMESTAMP;
    public static Expression X_DECIMAL;
    
    protected static String getUrl() {
        return TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
    }
    
    protected static String getUrl(String tenantId) {
        return getUrl() + ';' + TENANT_ID_ATTRIB + '=' + tenantId;
    }
    
    protected static PhoenixTestDriver driver;
    
    private static void startServer(String url) throws Exception {
        assertNull(driver);
        // only load the test driver if we are testing locally - for integration tests, we want to
        // test on a wider scale
        if (PhoenixEmbeddedDriver.isTestUrl(url)) {
            driver = initDriver(ReadOnlyProps.EMPTY_PROPS);
            assertTrue(DriverManager.getDriver(url) == driver);
            driver.connect(url, PropertiesUtil.deepCopy(TEST_PROPERTIES));
        }
    }
    
    protected static synchronized PhoenixTestDriver initDriver(ReadOnlyProps props) throws Exception {
        if (driver == null) {
            driver = new PhoenixTestDriver(props);
            DriverManager.registerDriver(driver);
        }
        return driver;
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        startServer(getUrl());
        ensureTableCreated(getUrl(), ATABLE_NAME);
        ensureTableCreated(getUrl(), ENTITY_HISTORY_TABLE_NAME);
        ensureTableCreated(getUrl(), FUNKY_NAME);
        ensureTableCreated(getUrl(), PTSDB_NAME);
        ensureTableCreated(getUrl(), PTSDB2_NAME);
        ensureTableCreated(getUrl(), PTSDB3_NAME);
        ensureTableCreated(getUrl(), MULTI_CF_NAME);
        ensureTableCreated(getUrl(), JOIN_ORDER_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_CUSTOMER_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_ITEM_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), JOIN_SUPPLIER_TABLE_FULL_NAME);
        ensureTableCreated(getUrl(), TABLE_WITH_ARRAY);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(HConstants.LATEST_TIMESTAMP));
        PhoenixConnection conn = DriverManager.getConnection(PHOENIX_CONNECTIONLESS_JDBC_URL, props).unwrap(PhoenixConnection.class);
        try {
            PTable table = conn.getMetaDataCache().getTable(new PTableKey(null, ATABLE_NAME));
            ATABLE = table;
            ORGANIZATION_ID = new ColumnRef(new TableRef(table), table.getColumn("ORGANIZATION_ID").getPosition()).newColumnExpression();
            ENTITY_ID = new ColumnRef(new TableRef(table), table.getColumn("ENTITY_ID").getPosition()).newColumnExpression();
            A_INTEGER = new ColumnRef(new TableRef(table), table.getColumn("A_INTEGER").getPosition()).newColumnExpression();
            A_STRING = new ColumnRef(new TableRef(table), table.getColumn("A_STRING").getPosition()).newColumnExpression();
            B_STRING = new ColumnRef(new TableRef(table), table.getColumn("B_STRING").getPosition()).newColumnExpression();
            A_DATE = new ColumnRef(new TableRef(table), table.getColumn("A_DATE").getPosition()).newColumnExpression();
            A_TIME = new ColumnRef(new TableRef(table), table.getColumn("A_TIME").getPosition()).newColumnExpression();
            A_TIMESTAMP = new ColumnRef(new TableRef(table), table.getColumn("A_TIMESTAMP").getPosition()).newColumnExpression();
            X_DECIMAL = new ColumnRef(new TableRef(table), table.getColumn("X_DECIMAL").getPosition()).newColumnExpression();
        } finally {
            conn.close();
        }
    }
    
    @AfterClass
    public static void doTeardown() throws Exception {
        if (driver != null) {
            try {
                driver.close();
            } finally {
                PhoenixTestDriver driver = BaseConnectionlessQueryTest.driver;
                BaseConnectionlessQueryTest.driver = null;
                DriverManager.deregisterDriver(driver);
            }
        }
    }

    protected static void assertRoundtrip(String sql) throws SQLException {
        SQLParser parser = new SQLParser(sql);
        BindableStatement stmt = null;
        stmt = parser.parseStatement();
        if (stmt.getOperation() != Operation.QUERY) {
            return;
        }
        String newSQL = stmt.toString();
        SQLParser newParser = new SQLParser(newSQL);
        BindableStatement newStmt = null;
        try {
            newStmt = newParser.parseStatement();
        } catch (SQLException e) {
            fail("Unable to parse new:\n" + newSQL);
        }
        assertEquals("Expected equality:\n" + sql + "\n" + newSQL, stmt, newStmt);
    }
}
