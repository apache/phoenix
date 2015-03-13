package org.apache.phoenix.calcite;

import com.google.common.collect.Lists;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * Integration test for queries powered by Calcite.
 */
public class CalciteTest extends BaseClientManagedTimeIT {
    public static final String ATABLE_NAME = "ATABLE";

    public static Start start() {
        return new Start();
    }

    public static class Start {
        private Connection connection;

        Connection createConnection() throws Exception {
            return CalciteTest.createConnection();
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

        public List<String> getResult(ResultSet resultSet) throws SQLException {
            final List<String> list = Lists.newArrayList();
            populateResult(resultSet, list);
            return list;
        }

        private void populateResult(ResultSet resultSet, List<String> list) throws SQLException {
            final StringBuilder buf = new StringBuilder();
            final int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) {
                        buf.append(", ");
                    }
                    buf.append(resultSet.getString(i + 1));
                }
                list.add(buf.toString());
                buf.setLength(0);
            }
        }

        public Sql explainIs(String expected) {
            final List<String> list = getResult("explain plan for " + sql);
            if (list.size() != 1) {
                fail("explain should return 1 row, got " + list.size());
            }
            String explain = list.get(0);
            assertThat(explain, equalTo(expected));
            return this;
        }

        public List<String> getResult(String sql) {
            try {
                final Statement statement = start.getConnection().createStatement();
                final ResultSet resultSet = statement.executeQuery(sql);
                List<String> list = getResult(resultSet);
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

        public Sql resultIs(String... lines) {
            assertThat(Arrays.asList(lines), equalTo(getResult(sql)));
            return this;
        }
    }

    private static Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection(
            "jdbc:calcite:");
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        final String url = getUrl();
        final PhoenixConnection phoenixConnection =
            DriverManager.getConnection(url).unwrap(PhoenixConnection.class);
        BaseTest.ensureTableCreated(url, ATABLE_NAME);
        calciteConnection.getRootSchema().add("phoenix",
            new PhoenixSchema(phoenixConnection));
        calciteConnection.setSchema("phoenix");
        return connection;
    }

    private static Connection connectUsingModel() throws Exception {
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
            DriverManager.getConnection("jdbc:calcite:model=" + file.getAbsolutePath());
        BaseTest.ensureTableCreated(url, ATABLE_NAME);
        return connection;
    }
    
    private void testConnect(String query, Object[][] expectedValues) throws Exception {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:");
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        final String url = getUrl();
        final PhoenixConnection phoenixConnection =
            DriverManager.getConnection(url).unwrap(PhoenixConnection.class);
        ensureTableCreated(url, ATABLE_NAME);
        initATableValues(getOrganizationId(), null, url);
        ensureTableCreated(url, JOIN_ITEM_TABLE_FULL_NAME);
        ensureTableCreated(url, JOIN_SUPPLIER_TABLE_FULL_NAME);
        initJoinTableValues(url, null, null);
        calciteConnection.getRootSchema().add("phoenix",
            new PhoenixSchema(phoenixConnection));
        calciteConnection.setSchema("phoenix");
        final Statement statement = calciteConnection.createStatement();
        final ResultSet resultSet = statement.executeQuery(query);

        for (int i = 0; i < expectedValues.length; i++) {
            assertTrue(resultSet.next());
            Object[] row = expectedValues[i];
            for (int j = 0; j < row.length; j++) {
                assertEquals(row[j], resultSet.getObject(j + 1));
            }
        }        
        assertFalse(resultSet.next());
        
        resultSet.close();
        statement.close();
        connection.close();
    }
    
    @Test public void testTableScan() throws Exception {
        testConnect("select * from aTable where a_string = 'a'", 
                new Object[][] {{"00D300000000XHP", "00A123122312312", "a"}, 
                                {"00D300000000XHP", "00A223122312312", "a"}, 
                                {"00D300000000XHP", "00A323122312312", "a"}, 
                                {"00D300000000XHP", "00A423122312312", "a"}});
    }
    
    @Test public void testProject() throws Exception {
        testConnect("select entity_id, a_string, organization_id from aTable where a_string = 'a'", 
                new Object[][] {{"00A123122312312", "a", "00D300000000XHP"}, 
                                {"00A223122312312", "a", "00D300000000XHP"}, 
                                {"00A323122312312", "a", "00D300000000XHP"}, 
                                {"00A423122312312", "a", "00D300000000XHP"}});
    }
    
    @Test public void testJoin() throws Exception {
        testConnect("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id where t1.a_string = 'a'", 
                new Object[][] {{"00A123122312312", "a", "00D300000000XHP"}, 
                                {"00A223122312312", "a", "00D300000000XHP"}, 
                                {"00A323122312312", "a", "00D300000000XHP"}, 
                                {"00A423122312312", "a", "00D300000000XHP"}});
//        testConnect("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"", 
//                new Object[][] {{"0000000001", "T1", "0000000001", "S1"}, 
//                                {"0000000002", "T2", "0000000001", "S1"}, 
//                                {"0000000003", "T3", "0000000002", "S2"}, 
//                                {"0000000004", "T4", "0000000002", "S2"},
//                                {"0000000005", "T5", "0000000005", "S5"},
//                                {"0000000006", "T6", "0000000006", "S6"}});
    }

    @Test public void testExplainPlanForSelectWhereQuery() {
        start()
            .sql("select * from aTable where a_string = 'a'")
            .explainIs(
                "PhoenixToEnumerableConverter\n"
                    + "  PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
            .close();
    }

    @Test public void testExplainProject() {
        start()
            .sql("select a_string, b_string from aTable where a_string = 'a'")
            .explainIs(
                "PhoenixToEnumerableConverter\n"
                    + "  PhoenixProject(A_STRING=[$2], B_STRING=[$3])\n"
                    + "    PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
            .close();
    }

    @Test public void testConnectUsingModel() throws Exception {
        final Start start = new Start() {
            @Override
            Connection createConnection() throws Exception {
                return connectUsingModel();
            }
        };
        start.sql("select * from aTable")
            .explainIs("PhoenixToEnumerableConverter\n"
                + "  PhoenixTableScan(table=[[HR, ATABLE]])\n")
            // .resultIs("Xx")
            .close();
    }
}
