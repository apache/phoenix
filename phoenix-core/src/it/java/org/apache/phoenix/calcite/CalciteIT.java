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
package org.apache.phoenix.calcite;

import com.google.common.collect.Lists;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

/**
 * Integration test for queries powered by Calcite.
 */
public class CalciteIT extends BaseClientManagedTimeIT {
    public static final String ATABLE_NAME = "ATABLE";

    public static Start start(boolean materializationEnabled) {
        return new Start(getConnectionProps(materializationEnabled));
    }
    
    public static Start start(Properties props) {
        return new Start(props);
    }

    public static class Start {
        protected final Properties props;
        private Connection connection;
        
        Start(Properties props) {
            this.props = props;
        }

        Connection createConnection() throws Exception {
            return DriverManager.getConnection(
                    "jdbc:phoenixcalcite:" 
                            + getUrl().substring(PhoenixRuntime.JDBC_PROTOCOL.length() + 1), 
                    props);
        }

        public Sql sql(String sql) {
            return new Sql(this, sql);
        }

        public Connection getConnection() {
            if (connection == null) {
                try {
                    connection = createConnection();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return connection;
        }

        public void close() {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /** Fluid class for a test that has specified a SQL query. */
    static class Sql {
        private final Start start;
        private final String sql;

        public Sql(Start start, String sql) {
            this.start = start;
            this.sql = sql;
        }

        public static List<Object[]> getResult(ResultSet resultSet) throws SQLException {
            final List<Object[]> list = Lists.newArrayList();
            populateResult(resultSet, list);
            return list;
        }

        private static void populateResult(ResultSet resultSet, List<Object[]> list) throws SQLException {
            final int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = resultSet.getObject(i + 1);
                }
                list.add(row);
            }
        }

        public Sql explainIs(String expected) throws SQLException {
            final List<Object[]> list = getResult("explain plan for " + sql);
            if (list.size() != 1) {
                fail("explain should return 1 row, got " + list.size());
            }
            String explain = (String) (list.get(0)[0]);
            assertEquals(explain, expected);
            return this;
        }


        public boolean execute() throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final boolean execute = statement.execute(sql);
            statement.close();
            return execute;
        }

        public List<Object[]> getResult(String sql) throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            List<Object[]> list = getResult(resultSet);
            resultSet.close();
            statement.close();
            return list;
        }

        public void close() {
            start.close();
        }

        public Sql resultIs(Object[]... expected) throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            for (int i = 0; i < expected.length; i++) {
                assertTrue(resultSet.next());
                Object[] row = expected[i];
                for (int j = 0; j < row.length; j++) {
                    assertEquals(row[j], resultSet.getObject(j + 1));
                }
            }        
            assertFalse(resultSet.next());
            resultSet.close();
            statement.close();
            return this;
        }
    }

    private static final String FOODMART_SCHEMA = "     {\n"
            + "       type: 'jdbc',\n"
            + "       name: 'foodmart',\n"
            + "       jdbcDriver: 'org.hsqldb.jdbcDriver',\n"
            + "       jdbcUser: 'FOODMART',\n"
            + "       jdbcPassword: 'FOODMART',\n"
            + "       jdbcUrl: 'jdbc:hsqldb:res:foodmart',\n"
            + "       jdbcCatalog: null,\n"
            + "       jdbcSchema: 'foodmart'\n"
            + "     }";
    
    private static final String PHOENIX_SCHEMA = "    {\n"
            + "      name: 'phoenix',\n"
            + "      type: 'custom',\n"
            + "      factory: 'org.apache.phoenix.calcite.PhoenixSchema$Factory',\n"
            + "      operand: {\n"
            + "        url: \"" + getUrl() + "\"\n"
            + "      }\n"
            + "    }";

    private static Connection connectUsingModel(Properties props) throws Exception {
        final File file = File.createTempFile("model", ".json");
        final String url = getUrl();
        final PrintWriter pw = new PrintWriter(new FileWriter(file));
        pw.print(
            "{\n"
                + "  version: '1.0',\n"
                + "  defaultSchema: 'HR',\n"
                + "  schemas: [\n"
                + "    {\n"
                + "      name: 'HR',\n"
                + "      type: 'custom',\n"
                + "      factory: 'org.apache.phoenix.calcite.PhoenixSchema$Factory',\n"
                + "      operand: {\n"
                + "        url: \"" + url + "\",\n"
                + "        user: \"scott\",\n"
                + "        password: \"tiger\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}\n");
        pw.close();
        final Connection connection =
            DriverManager.getConnection("jdbc:phoenixcalcite:model=" + file.getAbsolutePath(), props);
        return connection;
    }

    private static Connection connectWithHsqldbUsingModel(Properties props) throws Exception {
        final File file = File.createTempFile("model", ".json");
        final PrintWriter pw = new PrintWriter(new FileWriter(file));
        pw.print(
            "{\n"
                + "  version: '1.0',\n"
                + "  defaultSchema: 'phoenix',\n"
                + "  schemas: [\n"
                + PHOENIX_SCHEMA + ",\n"
                + FOODMART_SCHEMA + "\n"
                + "  ]\n"
                + "}\n");
        pw.close();
        final Connection connection =
            DriverManager.getConnection("jdbc:phoenixcalcite:model=" + file.getAbsolutePath(), props);
        return connection;
    }

    private static Properties getConnectionProps(boolean enableMaterialization) {
        Properties props = new Properties();
        props.setProperty(
                CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                Boolean.toString(enableMaterialization));
        props.setProperty(
                CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
                Boolean.toString(false));
        return props;
    }
    
    @Before
    public void initTable() throws Exception {
        final String url = getUrl();
        ensureTableCreated(url, ATABLE_NAME);
        initATableValues(getOrganizationId(), null, url);
        initJoinTableValues(url, null, null);
        initArrayTable();
        initSaltedTables();
        initMultiTenantTables();
        createIndices(
                "CREATE INDEX IDX1 ON aTable (a_string) INCLUDE (b_string, x_integer)",
                "CREATE INDEX IDX2 ON aTable (b_string) INCLUDE (a_string, y_integer)",
                "CREATE INDEX IDX_FULL ON aTable (b_string) INCLUDE (a_string, a_integer, a_date, a_time, a_timestamp, x_decimal, x_long, x_integer, y_integer, a_byte, a_short, a_float, a_double, a_unsigned_float, a_unsigned_double)",
                "CREATE VIEW v AS SELECT * from aTable where a_string = 'a'",
                "CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1");
        final Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_CUSTOMER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ITEM_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_SUPPLIER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ORDER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SCORES_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SALTED_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS IDX_" + SALTED_TABLE_NAME);
        connection.createStatement().execute("UPDATE STATISTICS IDX1");
        connection.createStatement().execute("UPDATE STATISTICS IDX2");
        connection.createStatement().execute("UPDATE STATISTICS IDX_FULL");
        connection.close();
    }
    
    protected void createIndices(String... indexDDL) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        for (String ddl : indexDDL) {
            try {
                conn.createStatement().execute(ddl);
            } catch (TableAlreadyExistsException e) {
            } catch (SequenceAlreadyExistsException e) {                
            }
        }
        conn.close();        
    }
    
    protected static final String SCORES_TABLE_NAME = "scores";
    
    protected void initArrayTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + SCORES_TABLE_NAME
                    + "(student_id INTEGER NOT NULL, subject_id INTEGER NOT NULL, scores INTEGER[] CONSTRAINT pk PRIMARY KEY (student_id, subject_id))");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + SCORES_TABLE_NAME
                    + " VALUES(?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 1);
            stmt.setArray(3, conn.createArrayOf("INTEGER", new Integer[] {85, 80, 82}));
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 1);
            stmt.setArray(3, null);
            stmt.execute();
            stmt.setInt(1, 3);
            stmt.setInt(2, 2);
            stmt.setArray(3, conn.createArrayOf("INTEGER", new Integer[] {87, 88, 80}));
            stmt.execute();
            conn.commit();
        } catch (TableAlreadyExistsException e) {
        }
        conn.close();        
    }
    
    protected static final String NOSALT_TABLE_NAME = "nosalt_test_table";
    protected static final String NOSALT_TABLE_SALTED_INDEX_NAME = "idxsalted_nosalt_test_table";
    protected static final String SALTED_TABLE_NAME = "salted_test_table";
    protected static final String SALTED_TABLE_NOSALT_INDEX_NAME = "idx_salted_test_table";
    protected static final String SALTED_TABLE_SALTED_INDEX_NAME = "idxsalted_salted_test_table";
    
    protected void initSaltedTables() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + NOSALT_TABLE_NAME + " (mypk0 INTEGER NOT NULL, mypk1 INTEGER NOT NULL, col0 INTEGER, col1 INTEGER CONSTRAINT pk PRIMARY KEY (mypk0, mypk1))");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + NOSALT_TABLE_NAME
                    + " VALUES(?, ?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setInt(3, 3);
            stmt.setInt(4, 4);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 3);
            stmt.setInt(3, 4);
            stmt.setInt(4, 5);
            stmt.execute();
            stmt.setInt(1, 3);
            stmt.setInt(2, 4);
            stmt.setInt(3, 5);
            stmt.setInt(4, 6);
            stmt.execute();
            conn.commit();
            
            conn.createStatement().execute("CREATE INDEX " + NOSALT_TABLE_SALTED_INDEX_NAME + " ON " + NOSALT_TABLE_NAME + " (col0) SALT_BUCKETS=4");
            conn.commit();
            
            conn.createStatement().execute(
                    "CREATE TABLE " + SALTED_TABLE_NAME + " (mypk0 INTEGER NOT NULL, mypk1 INTEGER NOT NULL, col0 INTEGER, col1 INTEGER CONSTRAINT pk PRIMARY KEY (mypk0, mypk1)) SALT_BUCKETS=4");
            stmt = conn.prepareStatement(
                    "UPSERT INTO " + SALTED_TABLE_NAME
                    + " VALUES(?, ?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setInt(3, 3);
            stmt.setInt(4, 4);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 3);
            stmt.setInt(3, 4);
            stmt.setInt(4, 5);
            stmt.execute();
            stmt.setInt(1, 3);
            stmt.setInt(2, 4);
            stmt.setInt(3, 5);
            stmt.setInt(4, 6);
            stmt.execute();
            conn.commit();
            
            conn.createStatement().execute("CREATE INDEX " + SALTED_TABLE_NOSALT_INDEX_NAME + " ON " + SALTED_TABLE_NAME + " (col0)");
            conn.createStatement().execute("CREATE INDEX " + SALTED_TABLE_SALTED_INDEX_NAME + " ON " + SALTED_TABLE_NAME + " (col1) INCLUDE (col0) SALT_BUCKETS=4");
            conn.commit();
        } catch (TableAlreadyExistsException e) {
        }
        conn.close();        
    }
    
    protected static final String MULTI_TENANT_TABLE = "multitenant_test_table";
    protected static final String MULTI_TENANT_TABLE_INDEX = "idx_multitenant_test_table";
    protected static final String MULTI_TENANT_VIEW1 = "multitenant_test_view1";
    protected static final String MULTI_TENANT_VIEW1_INDEX = "idx_multitenant_test_view1";
    protected static final String MULTI_TENANT_VIEW2 = "multitenant_test_view2";
    protected static final String MULTI_TENANT_VIEW2_INDEX = "idx_multitenant_test_view2";
    
    protected void initMultiTenantTables() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + MULTI_TENANT_TABLE + " (tenant_id VARCHAR NOT NULL, id VARCHAR NOT NULL, col0 INTEGER, col1 INTEGER, col2 INTEGER CONSTRAINT pk PRIMARY KEY (tenant_id, id)) MULTI_TENANT=true");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + MULTI_TENANT_TABLE
                    + " VALUES(?, ?, ?, ?, ?)");
            stmt.setString(1, "10");
            stmt.setString(2, "2");
            stmt.setInt(3, 3);
            stmt.setInt(4, 4);
            stmt.setInt(5, 5);
            stmt.execute();
            stmt.setString(1, "15");
            stmt.setString(2, "3");
            stmt.setInt(3, 4);
            stmt.setInt(4, 5);
            stmt.setInt(5, 6);
            stmt.execute();
            stmt.setString(1, "20");
            stmt.setString(2, "4");
            stmt.setInt(3, 5);
            stmt.setInt(4, 6);
            stmt.setInt(5, 7);
            stmt.execute();
            stmt.setString(1, "20");
            stmt.setString(2, "5");
            stmt.setInt(3, 6);
            stmt.setInt(4, 7);
            stmt.setInt(5, 8);
            stmt.execute();
            conn.commit();
            
            conn.createStatement().execute(
                    "CREATE INDEX " + MULTI_TENANT_TABLE_INDEX
                    + " ON " + MULTI_TENANT_TABLE + "(col1) INCLUDE (col0, col2)");
            conn.commit();
            
            conn.close();
            props.setProperty("TenantId", "10");
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE VIEW " + MULTI_TENANT_VIEW1
                    + " AS select * from " + MULTI_TENANT_TABLE);
            conn.commit();
            
            conn.createStatement().execute(
                    "CREATE INDEX " + MULTI_TENANT_VIEW1_INDEX
                    + " ON " + MULTI_TENANT_VIEW1 + "(col0)");
            conn.commit();
            
            conn.close();
            props.setProperty("TenantId", "20");
            conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE VIEW " + MULTI_TENANT_VIEW2
                    + " AS select * from " + MULTI_TENANT_TABLE + " where col2 > 7");
            conn.commit();
            
            conn.createStatement().execute(
                    "CREATE INDEX " + MULTI_TENANT_VIEW2_INDEX
                    + " ON " + MULTI_TENANT_VIEW2 + "(col0)");
            conn.commit();
        } catch (TableAlreadyExistsException e) {
        } finally {
            conn.close();
        }
    }
    
    @Test public void testTableScan() throws Exception {
        start(false).sql("select * from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}})
                .close();
    }
    
    @Test public void testProject() throws Exception {
        start(false).sql("select entity_id, a_string, organization_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
    }
    
    @Test public void testJoin() throws Exception {
        start(false).sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$4], A_STRING=[$2], ORGANIZATION_ID=[$3])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($4, $1), =($3, $0))], joinType=[inner])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"},
                          {"0000000005", "T5", "0000000005", "S5"},
                          {"0000000006", "T6", "0000000006", "S6"}})
                .close();
        
        start(false).sql("SELECT * FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND supp.name = 'S5'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT1=[$3], DISCOUNT2=[$4], supplier_id=[$5], DESCRIPTION=[$6], supplier_id0=[$7], NAME0=[$8], PHONE=[$9], ADDRESS=[$10], LOC_ID=[$11])\n" +
                           "    PhoenixServerJoin(condition=[=($5, $7)], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1], PHONE=[$2], ADDRESS=[$3], LOC_ID=[$4], NAME5=[CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], filter=[=(CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'S5')])\n")
                .resultIs(new Object[][] {
                          {"0000000005", "T5", 500, 8, 15, "0000000005", "Item T5", "0000000005", "S5", "888-888-5555", "505 YYY Street", "10005"}})
                .close();
        
        start(false).sql("SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o INNER JOIN " 
                + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" AND o.price = (i.price * (100 - discount2)) / 100.0 WHERE quantity < 5000")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(order_id=[$5], NAME=[$1], PRICE=[$2], DISCOUNT2=[$3], QUANTITY=[$7])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($6, $0), =($8, $4))], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT2=[$4], $f7=[/(*($2, -(100, $4)), 100.0)])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4], PRICE7=[CAST($3):DECIMAL(17, 6)])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[<($4, 5000)])\n")
                .resultIs(new Object[][] {
                          {"000000000000004", "T6", 600, 15, 4000}})
                .close();
    }
    
    @Test public void testRightOuterJoin() throws Exception {
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item RIGHT OUTER JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixServerJoin(condition=[=($4, $0)], joinType=[left])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"},
                          {null, null, "0000000003", "S3"}, 
                          {null, null, "0000000004", "S4"}, 
                          {"0000000005", "T5", "0000000005", "S5"},
                          {"0000000006", "T6", "0000000006", "S6"}})
                .close();
    }
    
    @Test public void testClientJoin() throws Exception {        
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item FULL OUTER JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by \"item_id\", supp.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientSort(sort0=[$0], sort1=[$3], dir0=[ASC], dir1=[ASC])\n" +
                           "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "      PhoenixClientJoin(condition=[=($2, $3)], joinType=[full])\n" +
                           "        PhoenixServerSort(sort0=[$2], dir0=[ASC])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {"0000000001", "T1", "0000000001", "S1"},
                        {"0000000002", "T2", "0000000001", "S1"},
                        {"0000000003", "T3", "0000000002", "S2"},
                        {"0000000004", "T4", "0000000002", "S2"},
                        {"0000000005", "T5", "0000000005", "S5"},
                        {"0000000006", "T6", "0000000006", "S6"},
                        {"invalid001", "INVALID-1", null, null},
                        {null, null, "0000000003", "S3"},
                        {null, null, "0000000004", "S4"}})
                .close();
        
        start(false).sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.organization_id = t2.organization_id and t1.entity_id = t2.entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$1], A_STRING=[$4], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixClientJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[inner])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"},
                          {"00A223122312312", "a", "00D300000000XHP"},
                          {"00A323122312312", "a", "00D300000000XHP"},
                          {"00A423122312312", "a", "00D300000000XHP"},
                          {"00B523122312312", "b", "00D300000000XHP"},
                          {"00B623122312312", "b", "00D300000000XHP"},
                          {"00B723122312312", "b", "00D300000000XHP"},
                          {"00B823122312312", "b", "00D300000000XHP"},
                          {"00C923122312312", "c", "00D300000000XHP"}})
                .close();
    }
    
    @Test public void testJoinPlanningWithCollation() throws Exception { 
        // Server-join with LHS sorted on order-by fields
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixServerJoin(condition=[=($4, $0)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .close();
        
        // Server-join with LHS reversely sorted on order-by fields
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by supp.\"supplier_id\" DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixServerJoin(condition=[=($4, $0)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[REVERSE])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .close();
        
        // Join key being order-by fields with the other side sorted on order-by fields
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4], supplier_id0=[$2])\n" +
                           "    PhoenixClientJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerSort(sort0=[$2], dir0=[ASC])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n")
                .close();
        
        start(false).sql("SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o LEFT JOIN " 
                + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                		   "  PhoenixClientProject(order_id=[$0], NAME=[$4], PRICE=[$5], DISCOUNT2=[$6], QUANTITY=[$2])\n" +
                		   "    PhoenixLimit(fetch=[2])\n" +
                		   "      PhoenixClientJoin(condition=[=($1, $3)], joinType=[left])\n" +
                		   "        PhoenixClientSort(sort0=[$1], dir0=[ASC])\n" +
                		   "          PhoenixLimit(fetch=[2])\n" +
                		   "            PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4])\n" +
                		   "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                		   "        PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT2=[$4])\n" +
                		   "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n")
                .close();
    }
    
    @Test public void testMultiJoin() throws Exception {
        start(false).sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$19], A_STRING=[$38], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($19, $1), =($18, $0))], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixClientProject(ORGANIZATION_ID=[$18], ENTITY_ID=[$19], A_STRING=[$20], B_STRING=[$21], A_INTEGER=[$22], A_DATE=[$23], A_TIME=[$24], A_TIMESTAMP=[$25], X_DECIMAL=[$26], X_LONG=[$27], X_INTEGER=[$28], Y_INTEGER=[$29], A_BYTE=[$30], A_SHORT=[$31], A_FLOAT=[$32], A_DOUBLE=[$33], A_UNSIGNED_FLOAT=[$34], A_UNSIGNED_DOUBLE=[$35], ORGANIZATION_ID0=[$0], ENTITY_ID0=[$1], A_STRING0=[$2], B_STRING0=[$3], A_INTEGER0=[$4], A_DATE0=[$5], A_TIME0=[$6], A_TIMESTAMP0=[$7], X_DECIMAL0=[$8], X_LONG0=[$9], X_INTEGER0=[$10], Y_INTEGER0=[$11], A_BYTE0=[$12], A_SHORT0=[$13], A_FLOAT0=[$14], A_DOUBLE0=[$15], A_UNSIGNED_FLOAT0=[$16], A_UNSIGNED_DOUBLE0=[$17])\n" +
                           "        PhoenixServerJoin(condition=[AND(=($19, $1), =($18, $0))], joinType=[inner])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start(false).sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$19], A_STRING=[$2], ORGANIZATION_ID=[$36])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($19, $1), =($18, $0))], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixServerJoin(condition=[AND(=($1, $19), =($0, $18))], joinType=[inner])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"},
                          {"00A223122312312", "a", "00D300000000XHP"},
                          {"00A323122312312", "a", "00D300000000XHP"},
                          {"00A423122312312", "a", "00D300000000XHP"},
                          {"00B523122312312", "b", "00D300000000XHP"},
                          {"00B623122312312", "b", "00D300000000XHP"},
                          {"00B723122312312", "b", "00D300000000XHP"},
                          {"00B823122312312", "b", "00D300000000XHP"},
                          {"00C923122312312", "c", "00D300000000XHP"}})
                .close();
    }
    
    @Test public void testAggregate() throws Exception {
        start(false).sql("select count(b_string) from atable")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT($3)])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {9L}})
                .close();
        
        start(false).sql("select organization_id, count(b_string) from atable group by organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT($3)], isOrdered=[true])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", 9L}})
                .close();
        
        start(false).sql("select organization_id, entity_id, count(b_string) from atable group by entity_id ,organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0, 1}], EXPR$2=[COUNT($3)], isOrdered=[true])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", 1L}, 
                          {"00D300000000XHP", "00A223122312312", 1L}, 
                          {"00D300000000XHP", "00A323122312312", 1L}, 
                          {"00D300000000XHP", "00A423122312312", 1L}, 
                          {"00D300000000XHP", "00B523122312312", 1L}, 
                          {"00D300000000XHP", "00B623122312312", 1L}, 
                          {"00D300000000XHP", "00B723122312312", 1L}, 
                          {"00D300000000XHP", "00B823122312312", 1L}, 
                          {"00D300000000XHP", "00C923122312312", 1L}})
                .close();
        
        start(false).sql("select entity_id, count(b_string) from atable group by entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{1}], EXPR$1=[COUNT($3)], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                        {"00A123122312312", 1L}, 
                        {"00A223122312312", 1L}, 
                        {"00A323122312312", 1L}, 
                        {"00A423122312312", 1L}, 
                        {"00B523122312312", 1L}, 
                        {"00B623122312312", 1L}, 
                        {"00B723122312312", 1L}, 
                        {"00B823122312312", 1L}, 
                        {"00C923122312312", 1L}})
                .close();
        
        start(false).sql("select a_string, count(b_string) from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT($3)], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"a", 4L},
                          {"b", 4L},
                          {"c", 1L}})
                .close();
        
        start(false).sql("select count(entity_id), a_string from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "a"},
                          {4L, "b"},
                          {1L, "c"}})
                .close();
        
        start(false).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "    PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S1", 2L},
                          {"S2", 2L},
                          {"S5", 1L},
                          {"S6", 1L}})
                .close();
        
        // test PhoenixOrderedAggregateRule
        start(false).sql("select s.\"supplier_id\", count(*) from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()], isOrdered=[true])\n" +
                           "    PhoenixServerJoin(condition=[=($0, $1)], joinType=[inner])\n" +
                           "      PhoenixServerProject(supplier_id=[$0])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], scanOrder=[FORWARD])\n" +
                           "      PhoenixServerProject(supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", 2L},
                          {"0000000002", 2L},
                          {"0000000005", 1L},
                          {"0000000006", 1L}})
                .close();
        
        start(false).sql("select a_string, sum(a_integer) from aTable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], EXPR$1=[SUM($4)], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                           {"a", 10L},
                           {"b", 26L},
                           {"c", 9L}})
                .close();
        
        start(false).sql("select mypk0, avg(mypk1) from " + SALTED_TABLE_NAME + " group by mypk0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(MYPK0=[$0], EXPR$1=[CAST(/($1, $2)):INTEGER NOT NULL])\n" +
                           "    PhoenixServerAggregate(group=[{0}], agg#0=[$SUM0($1)], agg#1=[COUNT()], isOrdered=[true])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {1, 2},
                        {2, 3},
                        {3, 4}})
                .close();
    }
    
    @Test public void testDistinct() throws Exception {
        start(false).sql("select distinct a_string from aTable")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], isOrdered=[false])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][]{
                          {"a"}, 
                          {"b"}, 
                          {"c"}})
                .close();
    }
    
    @Test public void testSort() throws Exception {
        start(false).sql("select organization_id, entity_id, a_string from aTable order by a_string, entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00C923122312312", "c"}})
                .close();
        
        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id, entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00C923122312312", "c"}})
                .close();
        
        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();
        
        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC, entity_id DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();

        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id ASC, entity_id DESC")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A123122312312", "a"}})
                .close();
        
        start(false).sql("select count(entity_id), a_string from atable group by a_string order by count(entity_id), a_string desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"},
                          {4L, "a"}})
                .close();
        
        start(false).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "    PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "      PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "        PhoenixServerProject(supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S6", 1L},
                          {"S5", 1L},
                          {"S2", 2L},
                          {"S1", 2L}})
                .close();
        
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000006", "T6", "0000000006", "S6"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000004", "T4", "0000000002", "S2"}, 
                          {"0000000003", "T3", "0000000002", "S2"},
                          {"0000000002", "T2", "0000000001", "S1"},
                          {"0000000001", "T1", "0000000001", "S1"}})
                .close();
    }
    
    @Test public void testSortWithLimit() throws Exception {
        start(false).sql("select organization_id, entity_id, a_string from aTable order by a_string, entity_id limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id, entity_id limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id DESC, entity_id DESC limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[REVERSE])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();

        start(false).sql("select organization_id, entity_id, a_string from aTable order by organization_id ASC, entity_id DESC limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00C923122312312", "c"},
                          {"00D300000000XHP", "00B823122312312", "b"}, 
                          {"00D300000000XHP", "00B723122312312", "b"}, 
                          {"00D300000000XHP", "00B623122312312", "b"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false).sql("select count(entity_id), a_string from atable group by a_string order by count(entity_id), a_string desc limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixLimit(fetch=[2])\n" +
                           "      PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "        PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"}})
                .close();
        
        start(false).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "        PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "          PhoenixServerProject(supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S6", 1L},
                          {"S5", 1L},
                          {"S2", 2L}})
                .close();
        
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixLimit(fetch=[3])\n" +
                           "      PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "        PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000006", "T6", "0000000006", "S6"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000004", "T4", "0000000002", "S2"}})
                .close();
    }
    
    @Test public void testLimit() throws Exception {
        start(false).sql("select organization_id, entity_id, a_string from aTable limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start(false).sql("select count(entity_id), a_string from atable group by a_string limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixLimit(fetch=[2])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()], isOrdered=[false])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "a"},
                          {4L, "b"}})
                .close();
        
        start(false).sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT()], isOrdered=[false])\n" +
                           "      PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "        PhoenixServerProject(supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S1", 2L},
                          {"S2", 2L},
                          {"S5", 1L}})
                .close();
        
        start(false).sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixLimit(fetch=[3])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}})
                .close();
        
        start(false).sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(X=[$0])\n" +
                           "    PhoenixLimit(fetch=[2])\n" +
                           "      PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIs(new Object[][] {{1}, {2}})
                .close();
    }

    @Test public void testScalarSubquery() throws Exception {
        start(false).sql("select \"item_id\", name, (select max(quantity) sq \n"
            + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
            + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                       "    PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                       "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                       "      PhoenixServerAggregate(group=[{0}], SQ=[MAX($5)], isOrdered=[true])\n" +
                       "        PhoenixServerJoin(condition=[=($3, $0)], joinType=[inner])\n" +
                       "          PhoenixServerProject(item_id=[$0])\n" +
                       "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                       "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n")
            .resultIs(new Object[][] {
                    new Object[] {"0000000001", "T1", 1000},
                    new Object[] {"0000000002", "T2", 3000},
                    new Object[] {"0000000003", "T3", 5000},
                    new Object[] {"0000000004", "T4", null},
                    new Object[] {"0000000005", "T5", null},
                    new Object[] {"0000000006", "T6", 4000},
                    new Object[] {"invalid001", "INVALID-1", null}})
            .close();
        
        start(false).sql("select \"item_id\", name, (select quantity sq \n"
                    + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
                    + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i where \"item_id\" < '0000000006'")
               .explainIs("PhoenixToEnumerableConverter\n" +
                          "  PhoenixClientProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                          "    PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                          "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n" +
                          "      PhoenixClientProject(item_id0=[$7], SQ=[$4])\n" +
                          "        PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                          "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "          PhoenixServerProject(item_id=[$0])\n" +
                          "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n")
               .resultIs(new Object[][] {
                         new Object[] {"0000000001", "T1", 1000},
                         new Object[] {"0000000002", "T2", 3000},
                         new Object[] {"0000000003", "T3", 5000},
                         new Object[] {"0000000004", "T4", null},
                         new Object[] {"0000000005", "T5", null}})
               .close();;
    }
    
    @Test public void testIndex() throws Exception {
        start(true).sql("select * from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(ORGANIZATION_ID=[$1], ENTITY_ID=[$2], A_STRING=[$3], B_STRING=[$0], A_INTEGER=[$4], A_DATE=[$5], A_TIME=[$6], A_TIMESTAMP=[$7], X_DECIMAL=[$8], X_LONG=[$9], X_INTEGER=[$10], Y_INTEGER=[$11], A_BYTE=[$12], A_SHORT=[$13], A_FLOAT=[$14], A_DOUBLE=[$15], A_UNSIGNED_FLOAT=[$16], A_UNSIGNED_DOUBLE=[$17])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start(true).sql("select x_integer from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(X_INTEGER=[$4])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]])\n")
            .close();
        start(true).sql("select a_string from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], scanOrder=[FORWARD])\n")
            .close();
        start(true).sql("select a_string from aTable order by organization_id")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, ATABLE]], scanOrder=[FORWARD])\n")
            .close();
        start(true).sql("select a_integer from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerSort(sort0=[$1], dir0=[ASC])\n" +
                       "    PhoenixServerProject(A_INTEGER=[$4], A_STRING=[$2])\n" +
                       "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
            .close();
        start(true).sql("select a_string, b_string from aTable where a_string = 'a'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$0], B_STRING=[$3])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], filter=[=($0, 'a')])\n")
            .close();
        start(true).sql("select a_string, b_string from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$3], B_STRING=[$0])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX2]], filter=[=($0, 'b')])\n")
            .close();
        start(true).sql("select a_string, b_string, x_integer, y_integer from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(A_STRING=[$3], B_STRING=[$0], X_INTEGER=[$10], Y_INTEGER=[$11])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start(true).sql("select a_string, count(*) from aTable group by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()], isOrdered=[true])\n" +
                       "    PhoenixTableScan(table=[[phoenix, IDX1]], scanOrder=[FORWARD])\n")
            .close();
    }
    
    @Test public void testValues() throws Exception {
        start(false).sql("select p0+p1 from (values (2, 1)) as t(p0, p1)")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientProject(EXPR$0=[+($0, $1)])\n" +
                       "    PhoenixValues(tuples=[[{ 2, 1 }]])\n")
            .close();
        start(false).sql("select count(p0), max(p1) from (values (2, 1), (3, 4), (5, 2)) as t(p0, p1)")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientAggregate(group=[{}], EXPR$0=[COUNT()], EXPR$1=[MAX($1)])\n" +
                       "    PhoenixValues(tuples=[[{ 2, 1 }, { 3, 4 }, { 5, 2 }]])\n")
            .resultIs(new Object[][] {{3L, 4}})
            .close();
    }
    
    @Test public void testUnion() throws Exception {
        start(false).sql("select entity_id from atable where a_string = 'a' union all select entity_id from atable where a_string = 'b'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixUnion(all=[true])\n" +
                           "    PhoenixServerProject(ENTITY_ID=[$1])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "    PhoenixServerProject(ENTITY_ID=[$1])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'b')])\n")
                .resultIs(new Object[][] {
                        {"00A123122312312"},
                        {"00A223122312312"},
                        {"00A323122312312"},
                        {"00A423122312312"},
                        {"00B523122312312"},
                        {"00B623122312312"},
                        {"00B723122312312"},
                        {"00B823122312312"}})
                .close();
        
        start(false).sql("select entity_id, a_string from atable where a_string = 'a' union all select entity_id, a_string from atable where a_string = 'c' order by entity_id desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixMergeSortUnion(all=[true])\n" +
                           "      PhoenixLimit(fetch=[3])\n" +
                           "        PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "          PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "      PhoenixLimit(fetch=[3])\n" +
                           "        PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "          PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'c')])\n")
                .resultIs(new Object[][] {
                        {"00C923122312312", "c"},
                        {"00A423122312312", "a"},
                        {"00A323122312312", "a"}})
                .close();
        
        start(false).sql("select entity_id, a_string from atable where a_string = 'a' union all select entity_id, a_string from atable where a_string = 'c' order by entity_id desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixMergeSortUnion(all=[true])\n" +
                           "    PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "      PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "    PhoenixServerSort(sort0=[$0], dir0=[DESC])\n" +
                           "      PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'c')])\n")
                .resultIs(new Object[][] {
                        {"00C923122312312", "c"},
                        {"00A423122312312", "a"},
                        {"00A323122312312", "a"},
                        {"00A223122312312", "a"},
                        {"00A123122312312", "a"}})
                .close();
    }
    
    @Test public void testUnnest() throws Exception {
        start(false).sql("SELECT t.s FROM UNNEST((SELECT scores FROM " + SCORES_TABLE_NAME + ")) AS t(s)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixUncollect\n" +
                           "    PhoenixServerProject(EXPR$0=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SCORES]])\n")
                .resultIs(new Object[][] {
                        {85}, 
                        {80}, 
                        {82}, 
                        {87}, 
                        {88}, 
                        {80}})
                .close();
        start(false).sql("SELECT s.student_id, t.score FROM " + SCORES_TABLE_NAME + " s, UNNEST((SELECT scores FROM " + SCORES_TABLE_NAME + " s2 where s.student_id = s2.student_id)) AS t(score)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(STUDENT_ID=[$0], SCORE=[$3])\n" +
                           "    PhoenixCorrelate(correlation=[$cor0], joinType=[INNER], requiredColumns=[{0}])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SCORES]])\n" +
                           "      PhoenixUncollect\n" +
                           "        PhoenixServerProject(EXPR$0=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, SCORES]], filter=[=($cor0.STUDENT_ID, $0)])\n")
                .resultIs(new Object[][] {
                        {1, 85}, 
                        {1, 80}, 
                        {1, 82}, 
                        {3, 87}, 
                        {3, 88}, 
                        {3, 80}})
                .close();
    }
    
    @Test public void testCorrelateAndDecorrelation() throws Exception {
        Properties correlProps = getConnectionProps(false);
        correlProps.setProperty("forceDecorrelate", Boolean.FALSE.toString());
        Properties decorrelProps = getConnectionProps(false);
        decorrelProps.setProperty("forceDecorrelate", Boolean.TRUE.toString());
        
        String q1 = "select \"order_id\", quantity from " + JOIN_ORDER_TABLE_FULL_NAME + " o where quantity = (select max(quantity) from " + JOIN_ORDER_TABLE_FULL_NAME + " o2 where o.\"item_id\" = o2.\"item_id\")";
        Object[][] r1 = new Object[][] {
                {"000000000000001", 1000},
                {"000000000000003", 3000},
                {"000000000000004", 4000},
                {"000000000000005", 5000}};
        String p1Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$0], QUANTITY=[$4])\n" +
                "    PhoenixFilter(condition=[=($4, $7)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{2}])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MAX($4)])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[=($cor0.item_id, $2)])\n";
        String p1Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$0], QUANTITY=[$4])\n" +
                "    PhoenixServerJoin(condition=[AND(=($2, $7), =($4, $8))], joinType=[inner])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "      PhoenixServerAggregate(group=[{7}], EXPR$0=[MAX($4)], isOrdered=[false])\n" +
                "        PhoenixServerJoin(condition=[=($7, $2)], joinType=[inner])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "          PhoenixServerAggregate(group=[{2}], isOrdered=[false])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(correlProps).sql(q1).explainIs(p1Correlate).resultIs(r1).close();
        start(decorrelProps).sql(q1).explainIs(p1Decorrelated).resultIs(r1).close();
                
        String q2 = "select name from " + JOIN_ITEM_TABLE_FULL_NAME + " i where price = (select max(price) from " + JOIN_ITEM_TABLE_FULL_NAME + " i2 where i.\"item_id\" = i2.\"item_id\" and i.name = i2.name and i2.\"item_id\" <> 'invalid001')";
        Object[][] r2 = new Object[][]{
                {"T1"},
                {"T2"},
                {"T3"},
                {"T4"},
                {"T5"},
                {"T6"}};
        String p2Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(NAME=[$1])\n" +
                "    PhoenixFilter(condition=[=($2, $7)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{0, 1}])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MAX($2)])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[AND(=($cor0.item_id, $0), =($cor0.NAME, $1), <>($0, 'invalid001'))])\n";
        String p2Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(NAME=[$1])\n" +
                "    PhoenixServerJoin(condition=[AND(=($0, $7), =($1, $8), =($2, $9))], joinType=[inner])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "      PhoenixServerAggregate(group=[{0, 1}], EXPR$0=[MAX($4)], isOrdered=[false])\n" +
                "        PhoenixServerJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[inner])\n" +
                "          PhoenixServerProject(item_id=[$0], NAME=[$1])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<>($0, 'invalid001')])\n";
        start(correlProps).sql(q2).explainIs(p2Correlate).resultIs(r2).close();
        start(decorrelProps).sql(q2).explainIs(p2Decorrelated).resultIs(r2).close();
        
        // Test PhoenixClientSemiJoin
        String q3 = "select \"item_id\", name from " + JOIN_ITEM_TABLE_FULL_NAME + " i where exists (select 1 from " + JOIN_ORDER_TABLE_FULL_NAME + " o where i.\"item_id\" = o.\"item_id\")";
        Object[][] r3 = new Object[][] {
                {"0000000001", "T1"},
                {"0000000002", "T2"},
                {"0000000003", "T3"},
                {"0000000006", "T6"}};
        String p3Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(item_id=[$0], NAME=[$1])\n" +
                "    PhoenixFilter(condition=[IS NOT NULL($7)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{0}])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], agg#0=[MIN($0)])\n" +
                "          PhoenixServerProject($f0=[true])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[=($cor0.item_id, $2)])\n";
        String p3Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(item_id=[$0], NAME=[$1])\n" +
                "    PhoenixClientSemiJoin(condition=[=($0, $7)], joinType=[inner])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                "      PhoenixClientProject(item_id0=[$0], $f0=[true])\n" +
                "        PhoenixServerJoin(condition=[=($0, $3)], joinType=[inner])\n" +
                "          PhoenixServerProject(item_id=[$0])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], scanOrder=[FORWARD])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(correlProps).sql(q3).explainIs(p3Correlate).resultIs(r3).close();
        start(decorrelProps).sql(q3).explainIs(p3Decorrelated).resultIs(r3).close();
        
        String q4 = "select \"item_id\", name from " + JOIN_ITEM_TABLE_FULL_NAME + " i where \"item_id\" in (select \"item_id\" from " + JOIN_ORDER_TABLE_FULL_NAME + ")";
        Object[][] r4 = new Object[][] {
                {"0000000001", "T1"},
                {"0000000002", "T2"},
                {"0000000003", "T3"},
                {"0000000006", "T6"}};
        String p4Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixServerSemiJoin(condition=[=($0, $4)], joinType=[inner])\n" +
                "    PhoenixServerProject(item_id=[$0], NAME=[$1])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "    PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(decorrelProps).sql(q4).explainIs(p4Decorrelated).resultIs(r4).close();
        
        // CALCITE-864: switching orders and items in the first join wouldn't work.
        String q5 = "select \"order_id\" from " + JOIN_ITEM_TABLE_FULL_NAME + " i JOIN " + JOIN_ORDER_TABLE_FULL_NAME + " o on o.\"item_id\" = i.\"item_id\" where quantity = (select max(quantity) from " + JOIN_ORDER_TABLE_FULL_NAME + " o2 JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " i2 on o2.\"item_id\" = i2.\"item_id\" where i.\"supplier_id\" = i2.\"supplier_id\")";
        Object [][] r5 = new Object[][] {
                {"000000000000003"},
                {"000000000000005"},
                {"000000000000004"}};
        String p5Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$7])\n" +
                "    PhoenixFilter(condition=[=($11, $14)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{5}])\n" +
                "        PhoenixServerJoin(condition=[=($9, $0)], joinType=[inner])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MAX($4)])\n" +
                "          PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[=($cor0.supplier_id, $5)])\n";
        String p5Decorrelated =
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(order_id=[$7])\n" +
                "    PhoenixServerJoin(condition=[AND(=($5, $14), =($9, $0))], joinType=[inner])\n" +
                "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "      PhoenixServerJoin(condition=[=($4, $8)], joinType=[inner])\n" +
                "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "        PhoenixServerAggregate(group=[{14}], EXPR$0=[MAX($4)], isOrdered=[false])\n" +
                "          PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                "            PhoenixServerJoin(condition=[=($7, $5)], joinType=[inner])\n" +
                "              PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "              PhoenixServerAggregate(group=[{5}], isOrdered=[false])\n" +
                "                PhoenixServerJoin(condition=[=($9, $0)], joinType=[inner])\n" +
                "                  PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                "                  PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n";
        start(correlProps).sql(q5).explainIs(p5Correlate).resultIs(r5).close();
        start(decorrelProps).sql(q5).explainIs(p5Decorrelated).resultIs(r5).close();
        
        String q6 = "select organization_id, entity_id, a_integer from v v1 where a_integer = (select min(a_integer) from v v2 where v1.organization_id = v2.organization_id and v1.b_string = v2.b_string)";
        Object[][] r6 = new Object[][] {
                {"00D300000000XHP", "00A123122312312", 1}, 
                {"00D300000000XHP", "00A223122312312", 2}, 
                {"00D300000000XHP", "00A323122312312", 3}};
        String p6Correlate = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_INTEGER=[$4])\n" +
                "    PhoenixFilter(condition=[=($4, $18)])\n" +
                "      PhoenixCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{0, 3}])\n" +
                "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                "        PhoenixServerAggregate(group=[{}], EXPR$0=[MIN($4)])\n" +
                "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[AND(=($2, 'a'), =($cor0.ORGANIZATION_ID, $0), =($cor0.B_STRING, $3))])\n";
        String p6Decorrelated = 
                "PhoenixToEnumerableConverter\n" +
                "  PhoenixClientProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_INTEGER=[$4])\n" +
                "    PhoenixServerJoin(condition=[AND(=($0, $18), =($3, $19), =($4, $20))], joinType=[inner])\n" +
                "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                "      PhoenixServerAggregate(group=[{18, 19}], EXPR$0=[MIN($4)], isOrdered=[false])\n" +
                "        PhoenixServerJoin(condition=[AND(=($18, $0), =($19, $3))], joinType=[inner])\n" +
                "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                "          PhoenixServerAggregate(group=[{0, 3}], isOrdered=[false])\n" +
                "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n";
        start(correlProps).sql(q6).explainIs(p6Correlate).resultIs(r6).close();
        start(decorrelProps).sql(q6).explainIs(p6Decorrelated).resultIs(r6).close();
    }
    
    @Test public void testInValueList() throws Exception {
        start(false).sql("select entity_id from aTable where organization_id = '00D300000000XHP' and entity_id in ('00A123122312312', '00A223122312312', '00B523122312312', '00B623122312312', '00C923122312312')")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerProject(ENTITY_ID=[$1])\n" +
                       "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[AND(=($0, '00D300000000XHP'), OR(=($1, '00A123122312312'), =($1, '00A223122312312'), =($1, '00B523122312312'), =($1, '00B623122312312'), =($1, '00C923122312312')))])\n")
            .resultIs(new Object[][] {
                    {"00A123122312312"},
                    {"00A223122312312"},
                    {"00B523122312312"},
                    {"00B623122312312"},
                    {"00C923122312312"}})
            .close();
    }
    
    @Test public void testSelectFromView() throws Exception {
        start(false).sql("select * from v")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                        {"00D300000000XHP", "00A123122312312", "a"}, 
                        {"00D300000000XHP", "00A223122312312", "a"}, 
                        {"00D300000000XHP", "00A323122312312", "a"}, 
                        {"00D300000000XHP", "00A423122312312", "a"}})
                .close();
    }
    
    @Test public void testSaltedIndex() throws Exception {
        start(true).sql("select count(*) from " + NOSALT_TABLE_NAME + " where col0 > 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_NOSALT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 3)])\n")
                .resultIs(new Object[][]{{2L}})
                .close();
        start(true).sql("select mypk0, mypk1, col0 from " + NOSALT_TABLE_NAME + " where col0 <= 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_NOSALT_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 4)])\n")
                .resultIs(new Object[][] {
                        {2, 3, 4},
                        {1, 2, 3}})
                .close();
        start(true).sql("select * from " + SALTED_TABLE_NAME + " where mypk0 < 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], filter=[<($0, 3)])\n")
                .resultIs(new Object[][] {
                        {1, 2, 3, 4},
                        {2, 3, 4, 5}})
                .close();
        start(true).sql("select count(*) from " + SALTED_TABLE_NAME + " where col0 > 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_SALTED_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 3)])\n")
                .resultIs(new Object[][]{{2L}})
                .close();
        start(true).sql("select mypk0, mypk1, col0 from " + SALTED_TABLE_NAME + " where col0 <= 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_SALTED_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 4)])\n")
                .resultIs(new Object[][] {
                        {2, 3, 4},
                        {1, 2, 3}})
                .close();
        start(true).sql("select count(*) from " + SALTED_TABLE_NAME + " where col1 > 4")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 4)])\n")
                .resultIs(new Object[][]{{2L}})
                .close();
        start(true).sql("select * from " + SALTED_TABLE_NAME + " where col1 <= 5 order by col1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[$3], COL1=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[<=(CAST($0):INTEGER, 5)], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {1, 2, 3, 4},
                        {2, 3, 4, 5}})
                .close();
        start(true).sql("select * from " + SALTED_TABLE_NAME + " s1, " + SALTED_TABLE_NAME + " s2 where s1.mypk0 = s2.mypk0 and s1.mypk1 = s2.mypk1 and s1.mypk0 > 1 and s2.col1 < 6")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerJoin(condition=[AND(=($0, $4), =($1, $5))], joinType=[inner])\n" +
                           "    PhoenixTableScan(table=[[phoenix, SALTED_TEST_TABLE]], filter=[>($0, 1)])\n" +
                           "    PhoenixServerProject(MYPK0=[$1], MYPK1=[$2], COL0=[$3], COL1=[CAST($0):INTEGER])\n" +
                           "      PhoenixTableScan(table=[[phoenix, IDXSALTED_SALTED_TEST_TABLE]], filter=[<(CAST($0):INTEGER, 6)])\n")
                .resultIs(new Object[][] {
                        {2, 3, 4, 5, 2, 3, 4, 5}})
                .close();
    }
    
    @Test public void testMultiTenant() throws Exception {
        Properties props = getConnectionProps(true);
        start(props).sql("select * from " + MULTI_TENANT_TABLE)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]])\n")
                .resultIs(new Object[][] {
                        {"10", "2", 3, 4, 5},
                        {"15", "3", 4, 5, 6},
                        {"20", "4", 5, 6, 7},
                        {"20", "5", 6, 7, 8}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where tenant_id = '20' and col1 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(TENANT_ID=[$0], ID=[$2], COL0=[$3], COL1=[CAST($1):INTEGER], COL2=[$4])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[AND(=(CAST($0):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, '20'), >(CAST($1):INTEGER, 1))])\n")
                .resultIs(new Object[][] {
                        {"20", "4", 5, 6, 7},
                        {"20", "5", 6, 7, 8}})
                .close();
        
        try {
            start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("")
                .close();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
        }
        
        props.setProperty("TenantId", "15");
        start(props).sql("select * from " + MULTI_TENANT_TABLE)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]])\n")
                .resultIs(new Object[][] {
                        {"3", 4, 5, 6}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where col1 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[$2], COL1=[CAST($0):INTEGER], COL2=[$3])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"3", 4, 5, 6}})
                .close();
        
        try {
            start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("")
                .close();
            fail("Should have got SQLException.");
        } catch (SQLException e) {
        }

        props.setProperty("TenantId", "10");
        start(props).sql("select * from " + MULTI_TENANT_VIEW1)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]])\n")
                .resultIs(new Object[][] {
                        {"2", 3, 4, 5}})
                .close();
        
        start(props).sql("select * from " + MULTI_TENANT_TABLE + " where col1 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[$2], COL1=[CAST($0):INTEGER], COL2=[$3])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_TABLE]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"2", 3, 4, 5}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_TABLE + " where col0 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_VIEW1]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"2", 3}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW1 + " where col0 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_VIEW1]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"2", 3}})
                .close();

        props.setProperty("TenantId", "20");
        start(props).sql("select * from " + MULTI_TENANT_VIEW2)
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, MULTITENANT_TEST_TABLE]], filter=[>($3, 7)])\n")
                .resultIs(new Object[][] {
                        {"5", 6, 7, 8}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW2 + " where col0 > 1")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_VIEW2]], filter=[>(CAST($0):INTEGER, 1)])\n")
                .resultIs(new Object[][] {
                        {"5", 6}})
                .close();
        
        start(props).sql("select id, col0 from " + MULTI_TENANT_VIEW2 + " order by col0")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ID=[$1], COL0=[CAST($0):INTEGER])\n" +
                           "    PhoenixTableScan(table=[[phoenix, IDX_MULTITENANT_TEST_VIEW2]], scanOrder=[FORWARD])\n")
                .resultIs(new Object[][] {
                        {"5", 6}})
                .close();
    }
    
    @Test public void testSequence() throws Exception {
        start(false).sql("select NEXT VALUE FOR seq, c0 from (values (1), (1)) as t(c0)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[NEXT_VALUE('\"SEQ\"')], C0=[$0])\n" +
                           "    PhoenixValues(tuples=[[{ 1 }, { 1 }]])\n")
                .resultIs(new Object[][]{
                        {1L, 1},
                        {2L, 1}})
                .close();

        start(false).sql("select NEXT VALUE FOR seq, entity_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[NEXT_VALUE('\"SEQ\"')], ENTITY_ID=[$1])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][]{
                        {3L, "00A123122312312"},
                        {4L, "00A223122312312"},
                        {5L, "00A323122312312"},
                        {6L, "00A423122312312"}})
                .close();
        
        start(false).sql("SELECT NEXT VALUE FOR seq, item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[NEXT_VALUE('\"SEQ\"')], item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                        {7L, "0000000001", "T1", "0000000001", "S1"}, 
                        {8L, "0000000002", "T2", "0000000001", "S1"}, 
                        {9L, "0000000003", "T3", "0000000002", "S2"}, 
                        {10L, "0000000004", "T4", "0000000002", "S2"},
                        {11L, "0000000005", "T5", "0000000005", "S5"},
                        {12L, "0000000006", "T6", "0000000006", "S6"}})
                .close();
    }

    /** Tests a simple command that is defined in Phoenix's extended SQL parser. 
     * @throws Exception */
    @Ignore
    @Test public void testCommit() throws Exception {
        start(false).sql("commit").execute();
    }

    @Test public void testCreateView() throws Exception {
        start(false).sql("create view v as select * from (values (1, 'a'), (2, 'b')) as t(x, y)").execute();
    }

    @Test public void testConnectJoinHsqldb() throws Exception {
        final Start start = new Start(getConnectionProps(false)) {
            @Override
            Connection createConnection() throws Exception {
                return connectWithHsqldbUsingModel(props);
            }
        };
        start.sql("select the_year, quantity as q, (select count(*) cnt \n"
            + "from \"foodmart\".\"time_by_day\" t where t.\"the_year\" = c.the_year)\n"
            + "from " + JOIN_ORDER_TABLE_FULL_NAME + " c")
            .explainIs("EnumerableCalc(expr#0..8=[{inputs}], THE_YEAR=[$t6], Q=[$t4], EXPR$2=[$t8])\n" +
                       "  EnumerableJoin(condition=[=($6, $7)], joinType=[left])\n" +
                       "    PhoenixToEnumerableConverter\n" +
                       "      PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "    EnumerableAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n" +
                       "      EnumerableAggregate(group=[{0}], CNT=[COUNT()])\n" +
                       "        EnumerableJoin(condition=[=($0, $11)], joinType=[inner])\n" +
                       "          PhoenixToEnumerableConverter\n" +
                       "            PhoenixServerAggregate(group=[{6}], isOrdered=[false])\n" +
                       "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "          JdbcToEnumerableConverter\n" +
                       "            JdbcProject(time_id=[$0], the_date=[$1], the_day=[$2], the_month=[$3], the_year=[$4], day_of_month=[$5], week_of_year=[$6], month_of_year=[$7], quarter=[$8], fiscal_period=[$9], the_year10=[CAST($4):INTEGER])\n" +
                       "              JdbcTableScan(table=[[foodmart, time_by_day]])\n")
            .resultIs(new Object[][] {
                    new Object[] {1997, 1000, 365L}, 
                    new Object[] {1997, 2000, 365L},
                    new Object[] {1997, 3000, 365L},
                    new Object[] {1998, 4000, 365L},
                    new Object[] {1998, 5000, 365L}})
            .close();;
    }

    @Test public void testConnectUsingModel() throws Exception {
        final Start start = new Start(getConnectionProps(false)) {
            @Override
            Connection createConnection() throws Exception {
                return connectUsingModel(props);
            }
        };
        start.sql("select * from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixTableScan(table=[[HR, ATABLE]])\n")
            // .resultIs("Xx")
            .close();
    }
}
