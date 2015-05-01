package org.apache.phoenix.calcite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.phoenix.end2end.BaseClientManagedTimeIT;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_FULL_NAME;
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
            "jdbc:phoenixcalcite:");
        final CalciteConnection calciteConnection =
            connection.unwrap(CalciteConnection.class);
        final String url = getUrl();
        Map<String, Object> operand = Maps.newHashMap();
        operand.put("url", url);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("phoenix",
            PhoenixSchema.FACTORY.create(rootSchema, null, operand));
        calciteConnection.setSchema("phoenix");
        return connection;
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
            DriverManager.getConnection("jdbc:phoenixcalcite:model=" + file.getAbsolutePath());
        return connection;
    }
    
    @Before
    public void initTable() throws Exception {
        final String url = getUrl();
        ensureTableCreated(url, ATABLE_NAME);
        initATableValues(getOrganizationId(), null, url);
        initJoinTableValues(url, null, null);
        final Connection connection = DriverManager.getConnection(url);
        connection.createStatement().execute("UPDATE STATISTICS ATABLE");
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_CUSTOMER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ITEM_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_SUPPLIER_TABLE_FULL_NAME);
        connection.createStatement().execute("UPDATE STATISTICS " + JOIN_ORDER_TABLE_FULL_NAME);
        connection.close();
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
        start().sql("select t1.entity_id, t2.a_string, t1.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ENTITY_ID=[$4], A_STRING=[$2], ORGANIZATION_ID=[$3])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($1, $4), =($0, $3))], joinType=[inner])\n" +
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
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\"")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
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
        
        start().sql("SELECT * FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" AND supp.name = 'S5'")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(item_id=[$0], NAME=[$1], PRICE=[$2], DISCOUNT1=[$3], DISCOUNT2=[$4], supplier_id=[$5], DESCRIPTION=[$6], supplier_id0=[$7], NAME0=[$8], PHONE=[$9], ADDRESS=[$10], LOC_ID=[$11])\n" +
                           "    PhoenixServerJoin(condition=[=($5, $7)], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "      PhoenixServerProject(supplier_id=[$0], NAME=[$1], PHONE=[$2], ADDRESS=[$3], LOC_ID=[$4], $f5=[CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL])\n" +
                           "        PhoenixTableScan(table=[[phoenix, Join, SupplierTable]], filter=[=(CAST($1):VARCHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL, 'S5')])\n")
                .resultIs(new Object[][] {
                          {"0000000005", "T5", 500, 8, 15, "0000000005", "Item T5", "0000000005", "S5", "888-888-5555", "505 YYY Street", "10005"}})
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
                           "        PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
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
    }
    
    @Test public void testMultiJoin() throws Exception {
        start().sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id where t1.a_string = 'a'") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ENTITY_ID=[$19], A_STRING=[$2], ORGANIZATION_ID=[$36])\n" +
                           "    PhoenixServerJoin(condition=[AND(=($1, $19), =($0, $18))], joinType=[inner])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "      PhoenixServerProject(ORGANIZATION_ID=[$18], ENTITY_ID=[$19], A_STRING=[$20], B_STRING=[$21], A_INTEGER=[$22], A_DATE=[$23], A_TIME=[$24], A_TIMESTAMP=[$25], X_DECIMAL=[$26], X_LONG=[$27], X_INTEGER=[$28], Y_INTEGER=[$29], A_BYTE=[$30], A_SHORT=[$31], A_FLOAT=[$32], A_DOUBLE=[$33], A_UNSIGNED_FLOAT=[$34], A_UNSIGNED_DOUBLE=[$35], ORGANIZATION_ID0=[$0], ENTITY_ID0=[$1], A_STRING0=[$2], B_STRING0=[$3], A_INTEGER0=[$4], A_DATE0=[$5], A_TIME0=[$6], A_TIMESTAMP0=[$7], X_DECIMAL0=[$8], X_LONG0=[$9], X_INTEGER0=[$10], Y_INTEGER0=[$11], A_BYTE0=[$12], A_SHORT0=[$13], A_FLOAT0=[$14], A_DOUBLE0=[$15], A_UNSIGNED_FLOAT0=[$16], A_UNSIGNED_DOUBLE0=[$17])\n" +
                           "        PhoenixServerJoin(condition=[AND(=($1, $19), =($0, $18))], joinType=[inner])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])\n")
                .resultIs(new Object[][] {
                          {"00A123122312312", "a", "00D300000000XHP"}, 
                          {"00A223122312312", "a", "00D300000000XHP"}, 
                          {"00A323122312312", "a", "00D300000000XHP"}, 
                          {"00A423122312312", "a", "00D300000000XHP"}})
                .close();
        
        start().sql("select t1.entity_id, t2.a_string, t3.organization_id from aTable t1 join aTable t2 on t1.entity_id = t2.entity_id and t1.organization_id = t2.organization_id join atable t3 on t1.entity_id = t3.entity_id and t1.organization_id = t3.organization_id") 
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerProject(ENTITY_ID=[$19], A_STRING=[$38], ORGANIZATION_ID=[$0])\n" +
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
    
    @Test public void testAggregate() {
        start().sql("select a_string, count(entity_id) from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
                           "    PhoenixServerProject(A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {"a", 4L},
                          {"b", 4L},
                          {"c", 1L}})
                .close();
        
        start().sql("select count(entity_id), a_string from atable group by a_string")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixClientProject(EXPR$0=[$1], A_STRING=[$0])\n" +
                           "    PhoenixServerAggregate(group=[{0}], EXPR$0=[COUNT()])\n" +
                           "      PhoenixServerProject(A_STRING=[$2])\n" +
                           "        PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "a"},
                          {4L, "b"},
                          {1L, "c"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
                           "    PhoenixServerProject(NAME=[$2])\n" +
                           "      PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "        PhoenixServerProject(supplier_id=[$5])\n" +
                           "          PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
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
                           "  PhoenixServerAggregate(group=[{0}])\n" +
                           "    PhoenixServerProject(A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
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
                           "  PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "    PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
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
                           "      PhoenixServerAggregate(group=[{0}], EXPR$0=[COUNT()])\n" +
                           "        PhoenixServerProject(A_STRING=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"},
                          {4L, "a"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "    PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
                           "      PhoenixServerProject(NAME=[$2])\n" +
                           "        PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "          PhoenixServerProject(supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
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
                           "  PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "    PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
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
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], statelessFetch=[5])\n")
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
                           "        PhoenixServerAggregate(group=[{0}], EXPR$0=[COUNT()])\n" +
                           "          PhoenixServerProject(A_STRING=[$2])\n" +
                           "            PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {1L, "c"},
                          {4L, "b"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name order by count(\"item_id\"), s.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixCompactClientSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[DESC])\n" +
                           "      PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
                           "        PhoenixServerProject(NAME=[$2])\n" +
                           "          PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "            PhoenixServerProject(supplier_id=[$5])\n" +
                           "              PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                           "            PhoenixServerProject(supplier_id=[$0], NAME=[$1])\n" +
                           "              PhoenixTableScan(table=[[phoenix, Join, SupplierTable]])\n")
                .resultIs(new Object[][] {
                          {"S6", 1L},
                          {"S5", 1L},
                          {"S2", 2L}})
                .close();
        
        start().sql("SELECT item.\"item_id\", item.name, supp.\"supplier_id\", supp.name FROM " + JOIN_ITEM_TABLE_FULL_NAME + " item JOIN " + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp ON item.\"supplier_id\" = supp.\"supplier_id\" order by item.name desc limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixServerSort(sort0=[$1], dir0=[DESC])\n" +
                           "      PhoenixServerProject(item_id=[$0], NAME=[$1], supplier_id=[$3], NAME0=[$4])\n" +
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
    
    @Test public void testLimit() {
        start().sql("select organization_id, entity_id, a_string from aTable limit 5")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[5])\n" +
                           "    PhoenixServerProject(ORGANIZATION_ID=[$0], ENTITY_ID=[$1], A_STRING=[$2])\n" +
                           "      PhoenixTableScan(table=[[phoenix, ATABLE]], statelessFetch=[5])\n")
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
                           "      PhoenixServerAggregate(group=[{0}], EXPR$0=[COUNT()])\n" +
                           "        PhoenixServerProject(A_STRING=[$2])\n" +
                           "          PhoenixTableScan(table=[[phoenix, ATABLE]])\n")
                .resultIs(new Object[][] {
                          {4L, "a"},
                          {4L, "b"}})
                .close();
        
        start().sql("select s.name, count(\"item_id\") from " + JOIN_SUPPLIER_TABLE_FULL_NAME + " s join " + JOIN_ITEM_TABLE_FULL_NAME + " i on s.\"supplier_id\" = i.\"supplier_id\" group by s.name limit 3")
                .explainIs("PhoenixToEnumerableConverter\n" +
                           "  PhoenixLimit(fetch=[3])\n" +
                           "    PhoenixServerAggregate(group=[{0}], EXPR$1=[COUNT()])\n" +
                           "      PhoenixServerProject(NAME=[$2])\n" +
                           "        PhoenixServerJoin(condition=[=($1, $0)], joinType=[inner])\n" +
                           "          PhoenixServerProject(supplier_id=[$5])\n" +
                           "            PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
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
    }
    
    @Test public void testSubquery() {
        start().sql("SELECT \"order_id\", quantity FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o WHERE quantity = (SELECT max(quantity) FROM " + JOIN_ORDER_TABLE_FULL_NAME + " q WHERE o.\"item_id\" = q.\"item_id\")")
               .explainIs("PhoenixToEnumerableConverter\n" +
                          "  PhoenixServerProject(order_id=[$0], QUANTITY=[$4])\n" +
                          "    PhoenixServerJoin(condition=[AND(=($2, $7), =($4, $8))], joinType=[inner])\n" +
                          "      PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "      PhoenixServerAggregate(group=[{0}], EXPR$0=[MAX($1)])\n" +
                          "        PhoenixServerProject(item_id0=[$7], QUANTITY=[$4])\n" +
                          "          PhoenixServerJoin(condition=[=($7, $2)], joinType=[inner])\n" +
                          "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                          "            PhoenixServerAggregate(group=[{0}])\n" +
                          "              PhoenixServerProject(item_id=[$2])\n" +
                          "                PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n")
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
                       "  PhoenixServerProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                       "    PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                       "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]])\n" +
                       "      PhoenixServerAggregate(group=[{0}], SQ=[MAX($1)])\n" +
                       "        PhoenixServerProject(item_id0=[$7], QUANTITY=[$4])\n" +
                       "          PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                       "            PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "            PhoenixServerAggregate(group=[{0}])\n" +
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
            .close();;
            start().sql("select \"item_id\", name, (select quantity sq \n"
                    + "from " + JOIN_ORDER_TABLE_FULL_NAME + " o where o.\"item_id\" = i.\"item_id\")\n"
                    + "from " + JOIN_ITEM_TABLE_FULL_NAME + " i where \"item_id\" < '0000000006'")
                    .explainIs("PhoenixToEnumerableConverter\n" +
                               "  PhoenixServerProject(item_id=[$0], NAME=[$1], EXPR$2=[$8])\n" +
                               "    PhoenixServerJoin(condition=[=($0, $7)], joinType=[left], isSingleValueRhs=[true])\n" +
                               "      PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n" +
                               "      PhoenixServerProject(item_id0=[$7], SQ=[$4])\n" +
                               "        PhoenixServerJoin(condition=[=($2, $7)], joinType=[inner])\n" +
                               "          PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                               "          PhoenixServerAggregate(group=[{0}])\n" +
                               "            PhoenixServerProject(item_id=[$0])\n" +
                               "              PhoenixTableScan(table=[[phoenix, Join, ItemTable]], filter=[<($0, '0000000006')])\n")
                    .resultIs(new Object[][] {
                            new Object[] {"0000000001", "T1", 1000}, 
                            new Object[] {"0000000002", "T2", 3000}, 
                            new Object[] {"0000000003", "T3", 5000}, 
                            new Object[] {"0000000004", "T4", null}, 
                            new Object[] {"0000000005", "T5", null}})
                    .close();;
    }
    
    @Test public void testConnectJoinHsqldb() {
        final Start start = new Start() {
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
                       "      PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "    EnumerableAggregate(group=[{0}], agg#0=[SINGLE_VALUE($1)])\n" +
                       "      EnumerableAggregate(group=[{0}], CNT=[COUNT()])\n" +
                       "        EnumerableCalc(expr#0..10=[{inputs}], expr#11=[0], expr#12=[CAST($t5):INTEGER], expr#13=[=($t12, $t0)], THE_YEAR=[$t0], $f0=[$t11], $condition=[$t13])\n" +
                       "          EnumerableJoin(condition=[true], joinType=[inner])\n" +
                       "            PhoenixToEnumerableConverter\n" +
                       "              PhoenixServerAggregate(group=[{0}])\n" +
                       "                PhoenixServerProject(THE_YEAR=[$6])\n" +
                       "                  PhoenixTableScan(table=[[phoenix, Join, OrderTable]])\n" +
                       "            JdbcToEnumerableConverter\n" +
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
