package org.apache.phoenix.calcite;

import com.google.common.collect.Lists;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.List;

import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
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

    private static Connection createConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection(
            "jdbc:calcite:");
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        final String url = getUrl();
        final PhoenixConnection phoenixConnection =
            DriverManager.getConnection(url).unwrap(PhoenixConnection.class);
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
        return connection;
    }
    
    @Before
    public void initTable() throws Exception {
        final String url = getUrl();
        ensureTableCreated(url, ATABLE_NAME);
        initATableValues(getOrganizationId(), null, url);
        initJoinTableValues(url, null, null);
    }
    
    @Test public void testTableScan() throws Exception {
        start().sql("select * from aTable where a_string = 'a'")
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
        start().sql("select entity_id, a_string, organization_id from aTable where a_string = 'a'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')], project=[[$1, $2, $0]])\n")
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
                           "  PhoenixProject(ENTITY_ID=[$4], A_STRING=[$2], ORGANIZATION_ID=[$3])\n" +
                           "    PhoenixJoin(condition=[AND(=($4, $1), =($3, $0))], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], project=[[$0, $1, $2]])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')], project=[[$0, $1, $2]])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
                           "    PhoenixJoin(condition=[=($2, $3)], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ITEMTABLE]], project=[[$0, $1, $5]])\n" +
                           "      PhoenixTableScan(table=[[phoenix, SUPPLIERTABLE]], project=[[$0, $1]])\n")
                .resultIs(new Object[][] {
                          {"0000000001", "T1", "0000000001", "S1"}, 
                          {"0000000002", "T2", "0000000001", "S1"}, 
                          {"0000000003", "T3", "0000000002", "S2"}, 
                          {"0000000004", "T4", "0000000002", "S2"},
                          {"0000000005", "T5", "0000000005", "S5"},
                          {"0000000006", "T6", "0000000006", "S6"}})
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
