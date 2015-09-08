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
import com.google.common.collect.Maps;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

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

    public static Start start() {
        return new Start(getConnectionProps(false), false);
    }
    
    public static Start start(Properties props, boolean connectUsingModel) {
        return new Start(props, connectUsingModel);
    }

    public static class Start {
        protected final Properties props;
        protected final boolean connectUsingModel;
        private Connection connection;
        
        Start(Properties props, boolean connectUsingModel) {
            this.props = props;
            this.connectUsingModel = connectUsingModel;
        }

        Connection createConnection() throws Exception {
            return connectUsingModel ? 
                    CalciteIT.connectUsingModel(props) 
                  : CalciteIT.createConnection(props);
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

        public Sql explainIs(String expected) {
            final List<Object[]> list = getResult("explain plan for " + sql);
            if (list.size() != 1) {
                fail("explain should return 1 row, got " + list.size());
            }
            String explain = (String) (list.get(0)[0]);
            assertEquals(explain, expected);
            return this;
        }


        public boolean execute() {
            try {
                final Statement statement = start.getConnection().createStatement();
                final boolean execute = statement.execute(sql);
                statement.close();
                return execute;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public List<Object[]> getResult(String sql) {
            try {
                final Statement statement = start.getConnection().createStatement();
                final ResultSet resultSet = statement.executeQuery(sql);
                List<Object[]> list = getResult(resultSet);
                resultSet.close();
                statement.close();
                return list;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            start.close();
        }

        public Sql resultIs(Object[]... expected) {
            try {
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
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
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
            DriverManager.getConnection("jdbc:phoenixcalcite:model=" + file.getAbsolutePath(), props == null ? new Properties() : props);
        return connection;
    }

    private static Connection createConnection(Properties props) throws SQLException {
        final Connection connection = DriverManager.getConnection(
            "jdbc:phoenixcalcite:", props);
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        final String url = getUrl();
        Map<String, Object> operand = Maps.newHashMap();
        operand.put("url", url);
        for (Entry<Object, Object> entry : props.entrySet()) {
            operand.put((String) entry.getKey(), entry.getValue());
        }
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("phoenix",
            PhoenixSchema.FACTORY.create(rootSchema, "phoenix", operand));
        calciteConnection.setSchema("phoenix");
        return connection;
    }

    private static Connection connectWithHsqldbUsingModel() throws Exception {
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
            DriverManager.getConnection("jdbc:phoenixcalcite:model=" + file.getAbsolutePath());
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
        createIndices(
                "CREATE INDEX IDX1 ON aTable (a_string) INCLUDE (b_string, x_integer)",
                "CREATE INDEX IDX2 ON aTable (b_string) INCLUDE (a_string, y_integer)",
                "CREATE INDEX IDX_FULL ON aTable (b_string) INCLUDE (a_string, a_integer, a_date, a_time, a_timestamp, x_decimal, x_long, x_integer, y_integer, a_byte, a_short, a_float, a_double, a_unsigned_float, a_unsigned_double)",
                "CREATE VIEW v AS SELECT * from aTable where a_string = 'a'");
        final Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_CUSTOMER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ITEM_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_SUPPLIER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ORDER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + SCORES_TABLE_NAME);
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
                    + "(student_id INTEGER PRIMARY KEY, scores INTEGER[])");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + SCORES_TABLE_NAME
                    + " VALUES(?, ?)");
            stmt.setInt(1, 1);
            stmt.setArray(2, conn.createArrayOf("INTEGER", new Integer[] {85, 80, 82}));
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setArray(2, null);
            stmt.execute();
            stmt.setInt(1, 3);
            stmt.setArray(2, conn.createArrayOf("INTEGER", new Integer[] {87, 88, 80}));
            stmt.execute();
            conn.commit();
        } catch (TableAlreadyExistsException e) {
        }
        conn.close();        
    }
    
    @Test public void testTableScan() throws Exception {
        start().sql("select * from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixToClientConverter\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}})
                .close();
    }
    
    @Test public void testProject() throws Exception {
        start().sql("select entity_id, a_string, organization_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixToClientConverter\n" +
                           "    PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
    }
    
    @Test public void testJoin() throws Exception {
        start().sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$4], A_STRING=[$2], ORGANIZATION_ID=[$3])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[AND(=($4, $1), =($3, $0))], joinType=[inner])\n" +
                           "        PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"},
                          {"0000000005", "T5", "0000000005", "S5"},
                          {"0000000006", "T6", "0000000006", "S6"}})
                .close();
        
        start().sql("SELECT * FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND supp.name = 'S5'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT1=[$3], DISCOUNT2=[$4], supplier_id=[$5], DESCRIPTION=[$6], supplier_id0=[$7], NAME0=[$8], PHONE=[$9], ADDRESS=[$10], LOC_ID=[$11])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[=($5, $7)], joinType=[inner])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1], PHONE=[$2], ADDRESS=[$3], LOC_ID=[$4], $f5=[CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], filter=[=(CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\", 'S5')])\n")
                .resultIs(new Object[][] {
                          {"0000000005", "T5", 500, 8, 15, "0000000005", "Item T5", "0000000005", "S5", "888-888-5555", "505 YYY Street", "10005"}})
                .close();
        
        start().sql("SELECT \"order_id\", i.name, i.price, discount2, quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o INNER JOIN " 
                + JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" AND o.price = (i.price * (100 - discount2)) / 100.0 WHERE quantity < 5000")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(order_id=[$5], NAME=[$1], PRICE=[$2], DISCOUNT2=[$3], QUANTITY=[$7])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[AND(=($6, $0), =($8, $4))], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT2=[$4], $f7=[/(*($2, -(100, $4)), 100.0)])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(order_id=[$0], item_id=[$2], QUANTITY=[$4], $f7=[CAST($3):DECIMAL(17, 6)])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]], filter=[<($4, 5000)])\n")
                .resultIs(new Object[][] {
                          {"000000000000004", "T6", 600, 15, 4000}})
                .close();
    }
    
    @Test public void testRightOuterJoin() throws Exception {
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item RIGHT OUTER JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[=($4, $0)], joinType=[left])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
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
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item FULL OUTER JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by \"item_id\", supp.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientSort(sort0=[$0], sort1=[$3], dir0=[ASC], dir1=[ASC])\n" +
                           "    PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "      PhoenixClientJoin(condition=[=($2, $3)], joinType=[full])\n" +
                           "        PhoenixServerSort(sort0=[$2], dir0=[ASC])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                        {null, null, "0000000003", "S3"},
                        {null, null, "0000000004", "S4"},
                        {"0000000001", "T1", "0000000001", "S1"},
                        {"0000000002", "T2", "0000000001", "S1"},
                        {"0000000003", "T3", "0000000002", "S2"},
                        {"0000000004", "T4", "0000000002", "S2"},
                        {"0000000005", "T5", "0000000005", "S5"},
                        {"0000000006", "T6", "0000000006", "S6"},
                        {"invalid001", "INVALID-1", null, null}})
                .close();
        
        start().sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.organization_id = t2.organization_id and t1.entity_id = t2.entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$1], A_STRING=[$4], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixClientJoin(condition=[AND(=($0, $2), =($1, $3))], joinType=[inner])\n" +
                           "      PhoenixToClientConverter\n" +
                           "        PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixToClientConverter\n" +
                           "        PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
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
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$2], NAME=[$3], supplier_id=[$0], NAME0=[$1])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[=($4, $0)], joinType=[inner])\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
                .close();
        
        // Join key being order-by fields with the other side sorted on order-by fields
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4], supplier_id0=[$2])\n" +
                           "    PhoenixClientJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixServerSort(sort0=[$2], dir0=[ASC])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixToClientConverter\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .close();
    }
    
    @Test public void testMultiJoin() throws Exception {
        start().sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$19], A_STRING=[$38], ORGANIZATION_ID=[$0])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[AND(=($19, $1), =($18, $0))], joinType=[inner])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "        PhoenixClientProject(ORGANIZATION_ID=[$18], ENTITY_ID=[$19], A_STRING=[$20], B_STRING=[$21], A_INTEGER=[$22], A_DATE=[$23], A_TIME=[$24], A_TIMESTAMP=[$25], X_DECIMAL=[$26], X_LONG=[$27], X_INTEGER=[$28], Y_INTEGER=[$29], A_BYTE=[$30], A_SHORT=[$31], A_FLOAT=[$32], A_DOUBLE=[$33], A_UNSIGNED_FLOAT=[$34], A_UNSIGNED_DOUBLE=[$35], ORGANIZATION_ID0=[$0], ENTITY_ID0=[$1], A_STRING0=[$2], B_STRING0=[$3], A_INTEGER0=[$4], A_DATE0=[$5], A_TIME0=[$6], A_TIMESTAMP0=[$7], X_DECIMAL0=[$8], X_LONG0=[$9], X_INTEGER0=[$10], Y_INTEGER0=[$11], A_BYTE0=[$12], A_SHORT0=[$13], A_FLOAT0=[$14], A_DOUBLE0=[$15], A_UNSIGNED_FLOAT0=[$16], A_UNSIGNED_DOUBLE0=[$17])\n" +
                           "          PhoenixToClientConverter\n" +
                           "            PhoenixServerJoin(condition=[AND(=($19, $1), =($18, $0))], joinType=[inner])\n" +
                           "              PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "              PhoenixToClientConverter\n" +
                           "                PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start().sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(ENTITY_ID=[$19], A_STRING=[$2], ORGANIZATION_ID=[$36])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerJoin(condition=[AND(=($19, $1), =($18, $0))], joinType=[inner])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerJoin(condition=[AND(=($1, $19), =($0, $18))], joinType=[inner])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "            PhoenixToClientConverter\n" +
                           "              PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
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
    
    @Test public void testAggregate() {
        start().sql("select a_string, count(b_string) from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}], EXPR$1=[COUNT($3)])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"a", 4L},
                          {"b", 4L},
                          {"c", 1L}})
                .close();
        
        start().sql("select count(entity_id), a_string from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "a"},
                          {4L, "b"},
                          {1L, "c"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{3}], EXPR$1=[COUNT()])\n" +
                           "    PhoenixServerJoin(condition=[=($2, $1)], joinType=[inner])\n" +
                           "      PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixToClientConverter\n" +
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S1", 2L},
                          {"S2", 2L},
                          {"S5", 1L},
                          {"S6", 1L}})
                .close();
    }
    
    @Test public void testDistinct() {
        start().sql("select distinct a_string from aTable")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{2}])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][]{
                          {"a"}, 
                          {"b"}, 
                          {"c"}})
                .close();
    }
    
    @Test public void testSort() {
        start().sql("select organization_id, entity_id, a_string from aTable order by a_string, entity_id")
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
        
        start().sql("select organization_id, entity_id, a_string from aTable order by organization_id, entity_id")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixToClientConverter\n" +
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
        
        start().sql("select count(entity_id), a_string from atable group by a_string order by count(entity_id), a_string desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"},
                          {4L, "a"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "    PhoenixServerAggregate(group=[{3}], EXPR$1=[COUNT()])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $1)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S6", 1L},
                          {"S5", 1L},
                          {"S2", 2L},
                          {"S1", 2L}})
                .close();
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000006", "T6", "0000000006", "S6"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000004", "T4", "0000000002", "S2"}, 
                          {"0000000003", "T3", "0000000002", "S2"},
                          {"0000000002", "T2", "0000000001", "S1"},
                          {"0000000001", "T1", "0000000001", "S1"}})
                .close();
    }
    
    @Test public void testSortWithLimit() {
        start().sql("select organization_id, entity_id, a_string from aTable order by a_string, entity_id limit 5")
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
        
        start().sql("select organization_id, entity_id, a_string from aTable order by organization_id, entity_id limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start().sql("select count(entity_id), a_string from atable group by a_string order by count(entity_id), a_string desc limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixLimit(fetch=[2])\n" +
                           "      PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "        PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{3}], EXPR$1=[COUNT()])\n" +
                           "        PhoenixServerJoin(condition=[=($2, $1)], joinType=[inner])\n" +
                           "          PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "          PhoenixToClientConverter\n" +
                           "            PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "              PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S6", 1L},
                          {"S5", 1L},
                          {"S2", 2L}})
                .close();
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixLimit(fetch=[3])\n" +
                           "      PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "        PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "          PhoenixToClientConverter\n" +
                           "            PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "              PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000006", "T6", "0000000006", "S6"}, 
                          {"0000000005", "T5", "0000000005", "S5"}, 
                          {"0000000004", "T4", "0000000002", "S2"}})
                .close();
    }
    
    @Test public void testLimit() {
        start().sql("select organization_id, entity_id, a_string from aTable limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"00D300000000XHP", "00A123122312312", "a"}, 
                          {"00D300000000XHP", "00A223122312312", "a"}, 
                          {"00D300000000XHP", "00A323122312312", "a"}, 
                          {"00D300000000XHP", "00A423122312312", "a"}, 
                          {"00D300000000XHP", "00B523122312312", "b"}})
                .close();
        
        start().sql("select count(entity_id), a_string from atable group by a_string limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixLimit(fetch=[2])\n" +
                           "      PhoenixServerAggregate(group=[{2}], EXPR$0=[COUNT()])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "a"},
                          {4L, "b"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixServerAggregate(group=[{3}], EXPR$1=[COUNT()])\n" +
                           "      PhoenixServerJoin(condition=[=($2, $1)], joinType=[inner])\n" +
                           "        PhoenixServerProject(item_id=[$0], supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S1", 2L},
                          {"S2", 2L},
                          {"S5", 1L}})
                .close();
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixLimit(fetch=[3])\n" +
                           "      PhoenixToClientConverter\n" +
                           "        PhoenixServerJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "          PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "          PhoenixToClientConverter\n" +
                           "            PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "              PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}})
                .close();
        
        start().sql("SELECT x from (values (1, 2), (2, 4), (3, 6)) as t(x, y) limit 2")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(X=[$0])\n" +
                           "    PhoenixLimit(fetch=[2])\n" +
                           "      PhoenixValues(tuples=[[{ 1, 2 }, { 2, 4 }, { 3, 6 }]])\n")
                .resultIs(new Object[][] {{1}, {2}})
                .close();
    }
    
    @Test public void testSubquery() {
        start().sql("SELECT \"order_id\", quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE quantity = (SELECT max(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\")")
               .explainIs("PhoenixToEnumerableConverter\n" +
                          "  PhoenixClientProject(order_id=[$0], QUANTITY=[$4])\n" +
                          "    PhoenixToClientConverter\n" +
                          "      PhoenixServerJoin(condition=[AND(=($2, $7), =($4, $8))], joinType=[inner])\n" +
                          "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "        PhoenixServerAggregate(group=[{7}], EXPR$0=[MAX($4)])\n" +
                          "          PhoenixServerJoin(condition=[=($7, $2)], joinType=[inner])\n" +
                          "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "            PhoenixServerAggregate(group=[{2}])\n" +
                          "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n")
               .resultIs(new Object[][]{
                         {"000000000000001", 1000},
                         {"000000000000003", 3000},
                         {"000000000000004", 4000},
                         {"000000000000005", 5000}})
               .close();
    }

    @Test public void testScalarSubquery() {
        start().sql("select \"item_id\", name, (select max(quantity) sq \n"
            + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
            + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                       "    PhoenixToClientConverter\n" +
                       "      PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                       "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                       "        PhoenixServerAggregate(group=[{7}], SQ=[MAX($4)])\n" +
                       "          PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                       "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "            PhoenixToClientConverter\n" +
                       "              PhoenixServerProject(item_id=[$0])\n" +
                       "                PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n")
            .resultIs(new Object[][] {
                    new Object[] {"0000000001", "T1", 1000},
                    new Object[] {"0000000002", "T2", 3000},
                    new Object[] {"0000000003", "T3", 5000},
                    new Object[] {"0000000004", "T4", null},
                    new Object[] {"0000000005", "T5", null},
                    new Object[] {"0000000006", "T6", 4000},
                    new Object[] {"invalid001", "INVALID-1", null}})
            .close();
        
        start().sql("select \"item_id\", name, (select quantity sq \n"
                    + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
                    + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i where \"item_id\" < '0000000006'")
               .explainIs("PhoenixToEnumerableConverter\n" +
                          "  PhoenixClientProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                          "    PhoenixToClientConverter\n" +
                          "      PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                          "        PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n" +
                          "        PhoenixClientProject(item_id0=[$7], SQ=[$4])\n" +
                          "          PhoenixToClientConverter\n" +
                          "            PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                          "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "              PhoenixServerAggregate(group=[{0}])\n" +
                          "                PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n")
               .resultIs(new Object[][] {
                         new Object[] {"0000000001", "T1", 1000},
                         new Object[] {"0000000002", "T2", 3000},
                         new Object[] {"0000000003", "T3", 5000},
                         new Object[] {"0000000004", "T4", null},
                         new Object[] {"0000000005", "T5", null}})
               .close();;
    }
    
    @Test public void testIndex() {
        final Start start = start(getConnectionProps(true), false);
        start.sql("select * from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                   "  PhoenixToClientConverter\n" +
                   "    PhoenixServerProject(ORGANIZATION_ID=[$1], ENTITY_ID=[$2], A_STRING=[$3], B_STRING=[$0], A_INTEGER=[$4], A_DATE=[$5], A_TIME=[$6], A_TIMESTAMP=[$7], X_DECIMAL=[$8], X_LONG=[$9], X_INTEGER=[$10], Y_INTEGER=[$11], A_BYTE=[$12], A_SHORT=[$13], A_FLOAT=[$14], A_DOUBLE=[$15], A_UNSIGNED_FLOAT=[$16], A_UNSIGNED_DOUBLE=[$17])\n" +
                   "      PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
        start.sql("select x_integer from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixServerProject(X_INTEGER=[$4])\n" +
                       "      PhoenixTableScan(table=[[phoenix, IDX1]])\n")
            .close();
        start.sql("select a_string from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixServerProject(A_STRING=[$0])\n" +
                       "      PhoenixTableScan(table=[[phoenix, IDX1]])\n")
            .close();
        start.sql("select a_string from aTable order by organization_id")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixServerProject(A_STRING=[$2], ORGANIZATION_ID=[$0])\n" +
                       "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
            .close();
        start.sql("select a_integer from aTable order by a_string")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixServerSort(sort0=[$1], dir0=[ASC])\n" +
                       "    PhoenixServerProject(A_INTEGER=[$4], A_STRING=[$2])\n" +
                       "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
            .close();
        start.sql("select a_string, b_string from aTable where a_string = 'a'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixServerProject(A_STRING=[$0], B_STRING=[$3])\n" +
                       "      PhoenixTableScan(table=[[phoenix, IDX1]], filter=[=($0, 'a')])\n")
            .close();
        start.sql("select a_string, b_string from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixServerProject(A_STRING=[$3], B_STRING=[$0])\n" +
                       "      PhoenixTableScan(table=[[phoenix, IDX2]], filter=[=($0, 'b')])\n")
            .close();
        start.sql("select a_string, b_string, x_integer, y_integer from aTable where b_string = 'b'")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixServerProject(A_STRING=[$3], B_STRING=[$0], X_INTEGER=[$10], Y_INTEGER=[$11])\n" +
                       "      PhoenixTableScan(table=[[phoenix, IDX_FULL]], filter=[=($0, 'b')])\n")
            .close();
    }
    
    @Test public void testValues() {
        start().sql("select p0+p1 from (values (2, 1)) as t(p0, p1)")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientProject(EXPR$0=[+($0, $1)])\n" +
                       "    PhoenixValues(tuples=[[{ 2, 1 }]])\n")
            .close();
        start().sql("select count(p0), max(p1) from (values (2, 1), (3, 4), (5, 2)) as t(p0, p1)")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixClientAggregate(group=[{}], EXPR$0=[COUNT()], EXPR$1=[MAX($1)])\n" +
                       "    PhoenixValues(tuples=[[{ 2, 1 }, { 3, 4 }, { 5, 2 }]])\n")
            .resultIs(new Object[][] {{3L, 4}})
            .close();
    }
    
    @Test public void testUnion() {
        start().sql("select entity_id from atable where a_string = 'a' union all select entity_id from atable where a_string = 'b'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixUnion(all=[true])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerProject(ENTITY_ID=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerProject(ENTITY_ID=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'b')])\n")
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
        
        start().sql("select entity_id, a_string from atable where a_string = 'a' union all select entity_id, a_string from atable where a_string = 'c' order by entity_id desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixClientSort(sort0=[$0], dir0=[DESC])\n" +
                           "      PhoenixUnion(all=[true])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n" +
                           "        PhoenixToClientConverter\n" +
                           "          PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'c')])\n")
                .resultIs(new Object[][] {
                        {"00C923122312312", "c"},
                        {"00A423122312312", "a"},
                        {"00A323122312312", "a"}})
                .close();
    }
    
    @Test public void testUnnest() {
        start().sql("SELECT t.s FROM UNNEST((SELECT scores FROM " + SCORES_TABLE_NAME + ")) AS t(s)")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixUncollect\n" +
                           "    PhoenixToClientConverter\n" +
                           "      PhoenixServerProject(EXPR$0=[$1])\n" +
                           "        PhoenixTableScan(table=[[phoenix, SCORES]])\n")
                .resultIs(new Object[][] {
                        {85}, 
                        {80}, 
                        {82}, 
                        {87}, 
                        {88}, 
                        {80}})
                .close();
    }
    
    @Test public void testSelectFromView() {
        start().sql("select * from v")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixToClientConverter\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                        {"00D300000000XHP", "00A123122312312", "a"}, 
                        {"00D300000000XHP", "00A223122312312", "a"}, 
                        {"00D300000000XHP", "00A323122312312", "a"}, 
                        {"00D300000000XHP", "00A423122312312", "a"}})
                .close();
    }

    /** Tests a simple command that is defined in Phoenix's extended SQL parser. */
    @Ignore
    @Test public void testCommit() {
        start().sql("commit").execute();
    }

    @Test public void testCreateView() {
        start().sql("create view v as select * from (values (1, 'a'), (2, 'b')) as t(x, y)").execute();
    }

    @Test public void testConnectJoinHsqldb() {
        final Start start = new Start(new Properties(), false) {
            @Override
            Connection createConnection() throws Exception {
                return connectWithHsqldbUsingModel();
            }
        };
        start.sql("select the_year, quantity as q, (select count(*) cnt \n"
            + "from \"foodmart\".\"time_by_day\" t where t.\"the_year\" = c.the_year)\n"
            + "from " + JOIN_ORDER_TABLE_FULL_NAME + " c")
            .explainIs("EnumerableCalc(expr#0..8=[{inputs}], THE_YEAR=[$t6], Q=[$t4], EXPR$2=[$t8])\n" +
                       "  EnumerableJoin(condition=[=($6, $7)], joinType=[left])\n" +
                       "    PhoenixToEnumerableConverter\n" +
                       "      PhoenixToClientConverter\n" +
                       "        PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "    EnumerableAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n" +
                       "      EnumerableAggregate(group=[{0}], CNT=[COUNT()])\n" +
                       "        EnumerableJoin(condition=[=($0, $11)], joinType=[inner])\n" +
                       "          PhoenixToEnumerableConverter\n" +
                       "            PhoenixServerAggregate(group=[{6}])\n" +
                       "              PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "          JdbcToEnumerableConverter\n" +
                       "            JdbcProject(time_id=[$0], the_date=[$1], the_day=[$2], the_month=[$3], the_year=[$4], day_of_month=[$5], week_of_year=[$6], month_of_year=[$7], quarter=[$8], fiscal_period=[$9], $f10=[CAST($4):INTEGER])\n" +
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
        final Start start = start(new Properties(), true);
        start.sql("select * from aTable")
            .explainIs("PhoenixToEnumerableConverter\n" +
                       "  PhoenixToClientConverter\n" +
                       "    PhoenixTableScan(table=[[HR, ATABLE]])\n")
            // .resultIs("Xx")
            .close();
    }
}
