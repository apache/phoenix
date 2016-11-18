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

import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.BaseJoinIT;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BaseCalciteIT extends BaseHBaseManagedTimeIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        props.put(QueryServices.STATS_USE_CURRENT_TIME_ATTRIB, Boolean.FALSE.toString());
        props.put(QueryServices.RUN_UPDATE_STATS_ASYNC, Boolean.FALSE.toString());
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(1000));
        props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(200));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    public static Start start(boolean materializationEnabled, float rowCountFactor) {
        return new Start(getConnectionProps(materializationEnabled, rowCountFactor));
    }
    
    public static Start start(Properties props) {
        return new Start(props);
    }
    
    public static Start startPhoenixStandalone(Properties props) {
        return new Start(props) {
            Connection createConnection() throws Exception {
                return DriverManager.getConnection(
                        getOldUrl(), 
                        props);
            }
            
            String getExplainPlanString() {
                return "explain";
            }
        };
    }

    public static class Start {
        protected final Properties props;
        private Connection connection;
        
        Start(Properties props) {
            this.props = props;
        }

        Connection createConnection() throws Exception {
        	// FIXME Cannot get correct stats with 'test=true' property
        	final String testProp = PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        	final String url = getUrl().replaceAll(testProp, "");
            return DriverManager.getConnection(url, props);
        }
        
        String getExplainPlanString(boolean json) {
            if (json) {
                return "explain plan as json for ";
            } else {
                return "explain plan for ";
            }
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

        public void commit() {
            if (connection != null) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void close() {
            if (connection != null) {
                try {
                    connection.commit();
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
            Assert.assertEquals(expected, getExplainPlan(false));
            return this;
        }

        public Sql explainIs(Matcher<PlanRelNode> planRelNodeMatcher) throws SQLException {
            String explainPlan = getExplainPlan(true);
            Assert.assertThat(PlanRelNode.parsePlan(explainPlan), planRelNodeMatcher);
            return this;
        }

        public Sql explainMatches(String expected) throws SQLException {
            String explainPlan = getExplainPlan(false);
            Assert.assertTrue(
                    "Explain plan \"" + explainPlan
                            + "\" does not match \"" + expected + "\"",
                    explainPlan.matches(expected));
            return this;
        }

        private String getExplainPlan(boolean json) throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(
                    start.getExplainPlanString(json) + " " + sql);
            String explainPlan = QueryUtil.getExplainPlan(resultSet);
            resultSet.close();
            statement.close();
            return explainPlan;
        }

        public List<Object[]> getResult() throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            final List<Object[]> list = Lists.newArrayList();
            populateResult(resultSet, list);
            resultSet.close();
            statement.close();
            return list;
        }

        public Sql execute() throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            statement.execute(sql);
            statement.close();
            return this;
        }
        
        public Sql executeUpdate() throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            statement.executeUpdate(sql);
            statement.close();
            return this;
        }
        
        public PreparedStatement prepareStatement() throws SQLException {
            return start.getConnection().prepareStatement(sql);
        }

        public void commit() {
            start.commit();
        }

        public void close() {
            start.close();
        }

        public Sql resultIs(Object[][] expected) throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            checkResultOrdered(resultSet, expected);
            resultSet.close();
            statement.close();
            return this;
        }

        public Sql resultIs(int orderedCount, Object[][] expected) throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            checkResultUnordered(resultSet, expected, orderedCount, null);
            resultSet.close();
            statement.close();
            return this;
        }

        public Sql resultIsSomeOf(int count, Object[][] expected) throws SQLException {
            final Statement statement = start.getConnection().createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            checkResultUnordered(resultSet, expected, 0, count);
            resultSet.close();
            statement.close();
            return this;
        }
        
        public Sql sameResultAsPhoenixStandalone() throws SQLException {
            Start phoenixStart = startPhoenixStandalone(this.start.props);
            List<Object[]> result = phoenixStart.sql(this.sql).getResult();
            phoenixStart.close();
            return resultIs(result.toArray(new Object[result.size()][]));
        }
        
        public Sql sameResultAsPhoenixStandalone(int orderedCount) throws SQLException {
            Start phoenixStart = startPhoenixStandalone(this.start.props);
            List<Object[]> result = phoenixStart.sql(this.sql).getResult();
            phoenixStart.close();
            return resultIs(orderedCount, result.toArray(new Object[result.size()][]));
        }
        
        private void checkResultOrdered(ResultSet resultSet, Object[][] expected) throws SQLException {
            int expectedCount = expected.length;
            int count = 0;
            for (int i = 0; i < expectedCount; i++) {
                assertTrue(
                        "Expected " + expectedCount + " rows, but got " + count + " rows.",
                        resultSet.next());
                count++;
                Object[] row = expected[i];
                for (int j = 0; j < row.length; j++) {
                    Object obj = resultSet.getObject(j + 1);
                    Assert.assertEquals(canonicalize(row[j]), canonicalize(obj));
                }
            }
            assertFalse("Got more rows than expected.", resultSet.next());            
        }
        
        private void checkResultUnordered(ResultSet resultSet, Object[][] expected, int orderedCount, Integer checkContains) throws SQLException {
            List<List<Object>> expectedResults = Lists.newArrayList();
            List<List<Object>> actualResults = Lists.newArrayList();
            List<List<Object>> errorResults = Lists.newArrayList();
            int columnCount = expected.length > 0 ? expected[0].length : 0;
            for (Object[] e : expected) {
                List<Object> row = Lists.newArrayList();
                for (int i = orderedCount; i < e.length; i++) {
                    row.add(canonicalize(e[i]));
                }
                expectedResults.add(row);
            }
            while (resultSet.next()) {
                if (actualResults.size() >= expected.length) {
                    fail("Got more rows than expected after getting results: " + actualResults);
                }
                // check the ordered part
                Object[] row = expected[actualResults.size()];
                for (int i = 0; i < orderedCount; i++) {
                    Object obj = resultSet.getObject(i + 1);
                    Assert.assertEquals(canonicalize(row[i]), canonicalize(obj));
                }
                // check the unordered part
                List<Object> result = Lists.newArrayList();
                for (int i = orderedCount; i < columnCount; i++) {
                    result.add(canonicalize(resultSet.getObject(i+1)));
                }
                if (!expectedResults.remove(result)) {
                    errorResults.add(result);
                }
                actualResults.add(result);
            }
            boolean allContainedInExpected = errorResults.isEmpty();
            boolean allExpectedFound = checkContains == null ? expectedResults.isEmpty() : checkContains == actualResults.size();
            assertTrue(
                    (allContainedInExpected ? "" : "Could not find " + errorResults + " in expected results.\n") +
                    (allExpectedFound ? "" : 
                        (checkContains == null
                              ? ("Count not find " + expectedResults + " in actual results: " + actualResults + ".\n")
                              : ("Expected " + checkContains + " rows, but got " + actualResults.size() + " rows."))),
                    allContainedInExpected && allExpectedFound);
        }
        
        private Object canonicalize(Object obj) {
            if (obj == null) {
                return obj;
            }
            
            if (obj instanceof ArrayImpl) {
                return obj.toString();
            }
            
            if (obj.getClass().isArray()) {
                final StringBuilder buf = new StringBuilder("[");
                for (Object o : (Object[]) obj) {
                    if (buf.length() > 1) {
                        buf.append(", ");
                    }
                    String s = o.toString();
                    // Remove nano suffix
                    if (o instanceof Timestamp) {
                        s = s.substring(0, s.lastIndexOf('.'));
                    }
                    buf.append(s);
                }
                return buf.append("]").toString();
            }
            
            return obj;
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
    
    private static final String getPhoenixSchema() {
        return "    {\n"
            + "      name: 'phoenix',\n"
            + "      type: 'custom',\n"
            + "      factory: 'org.apache.phoenix.calcite.PhoenixSchema$Factory',\n"
            + "      operand: {\n"
            + "        url: \"" + getOldUrl() + "\"\n"
            + "      }\n"
            + "    }";
    }

    protected static Connection connectUsingModel(Properties props) throws Exception {
        final File file = File.createTempFile("model", ".json");
        final String url = getOldUrl();
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

    protected static Connection connectWithHsqldbUsingModel(Properties props) throws Exception {
        final File file = File.createTempFile("model", ".json");
        final PrintWriter pw = new PrintWriter(new FileWriter(file));
        pw.print(
            "{\n"
                + "  version: '1.0',\n"
                + "  defaultSchema: 'phoenix',\n"
                + "  schemas: [\n"
                + getPhoenixSchema() + ",\n"
                + FOODMART_SCHEMA + "\n"
                + "  ]\n"
                + "}\n");
        pw.close();
        final Connection connection =
            DriverManager.getConnection("jdbc:phoenixcalcite:model=" + file.getAbsolutePath(), props);
        return connection;
    }

    protected static Properties getConnectionProps(boolean enableMaterialization, float rowCountFactor) {
        Properties props = new Properties();
        if (!enableMaterialization) {
            props.setProperty(
                    CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                    Boolean.toString(enableMaterialization));
        }
        props.setProperty(
                CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
                Boolean.toString(false));
        props.setProperty(PhoenixRel.ROW_COUNT_FACTOR, Float.toString(rowCountFactor));
        return props;
    }
    
    protected static final String SCORES_TABLE_NAME = "scores";
    
    protected void initJoinTableValues(String url) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            for (String tableName : new String[] {
                    BaseJoinIT.JOIN_CUSTOMER_TABLE_FULL_NAME,
                    BaseJoinIT.JOIN_ITEM_TABLE_FULL_NAME,
                    BaseJoinIT.JOIN_ORDER_TABLE_FULL_NAME,
                    BaseJoinIT.JOIN_SUPPLIER_TABLE_FULL_NAME}) {
                try {
                    BaseJoinIT.createTable(conn, tableName, tableName);
                } catch (TableAlreadyExistsException e) {
                }
                BaseJoinIT.initValues(conn, tableName, tableName);
            }
        } finally {
            conn.close();
        }
    }
    
    protected void initArrayTable(String url) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + SCORES_TABLE_NAME
                    + "(student_id INTEGER NOT NULL, subject_id INTEGER NOT NULL, scores INTEGER[], exam_date DATE[], exam_time TIME[], exam_timestamp TIMESTAMP[] CONSTRAINT pk PRIMARY KEY (student_id, subject_id))");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + SCORES_TABLE_NAME
                    + " VALUES(?, ?, ?, ?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 1);
            stmt.setArray(3, conn.createArrayOf("INTEGER", new Integer[] {85, 80, 82}));
            stmt.setArray(4, conn.createArrayOf("DATE", new Date[] {Date.valueOf("2016-3-22"), Date.valueOf("2016-5-23"), Date.valueOf("2016-7-24")}));
            stmt.setArray(5, conn.createArrayOf("TIME", new Time[] {Time.valueOf("15:30:28"), Time.valueOf("13:26:50"), Time.valueOf("16:20:00")}));
            stmt.setArray(6, conn.createArrayOf("TIMESTAMP", new Timestamp[] {Timestamp.valueOf("2016-3-22 15:30:28"), Timestamp.valueOf("2016-5-23 13:26:50"), Timestamp.valueOf("2016-7-24 16:20:00")}));
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 1);
            stmt.setArray(3, null);
            stmt.setArray(4, null);
            stmt.setArray(5, null);
            stmt.setArray(6, null);
            stmt.execute();
            stmt.setInt(1, 3);
            stmt.setInt(2, 2);
            stmt.setArray(3, conn.createArrayOf("INTEGER", new Integer[] {87, 88, 80}));
            stmt.setArray(4, conn.createArrayOf("DATE", new Date[] {Date.valueOf("2016-3-22"), Date.valueOf("2016-5-23"), Date.valueOf("2016-7-24")}));
            stmt.setArray(5, conn.createArrayOf("TIME", new Time[] {Time.valueOf("15:30:16"), Time.valueOf("13:26:52"), Time.valueOf("16:20:40")}));
            stmt.setArray(6, conn.createArrayOf("TIMESTAMP", new Timestamp[] {Timestamp.valueOf("2016-3-22 15:30:16"), Timestamp.valueOf("2016-5-23 13:26:52"), Timestamp.valueOf("2016-7-24 16:20:40")}));
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
    
    protected void initSaltedTables(String url, String index) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + NOSALT_TABLE_NAME + " (mypk0 INTEGER NOT NULL, mypk1 INTEGER NOT NULL, col0 INTEGER, col1 INTEGER CONSTRAINT pk PRIMARY KEY (mypk0, mypk1))");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + NOSALT_TABLE_NAME
                    + " VALUES(?, ?, ?, ?)");
            for (int i = 0; i < 1000; i++) {
                stmt.setInt(1, i + 1);
                stmt.setInt(2, i + 2);
                stmt.setInt(3, i + 3);
                stmt.setInt(4, i + 4);
                stmt.execute();
            }
            conn.commit();
            
            if (index != null) {
                conn.createStatement().execute(
                        "CREATE " + index + " " + NOSALT_TABLE_SALTED_INDEX_NAME + " ON " + NOSALT_TABLE_NAME + " (col0)"
                        + (index.toUpperCase().startsWith("LOCAL") ? "" : " SALT_BUCKETS=4"));
                conn.commit();
            }
            
            conn.createStatement().execute(
                    "CREATE TABLE " + SALTED_TABLE_NAME + " (mypk0 INTEGER NOT NULL, mypk1 INTEGER NOT NULL, col0 INTEGER, col1 INTEGER CONSTRAINT pk PRIMARY KEY (mypk0, mypk1)) SALT_BUCKETS=4");
            stmt = conn.prepareStatement(
                    "UPSERT INTO " + SALTED_TABLE_NAME
                    + " VALUES(?, ?, ?, ?)");
            for (int i = 0; i < 1000; i++) {
                stmt.setInt(1, i + 1);
                stmt.setInt(2, i + 2);
                stmt.setInt(3, i + 3);
                stmt.setInt(4, i + 4);
                stmt.execute();
            }
            conn.commit();
            
            if (index != null) {
                conn.createStatement().execute("CREATE " + index + " " + SALTED_TABLE_NOSALT_INDEX_NAME + " ON " + SALTED_TABLE_NAME + " (col0)");
                conn.createStatement().execute(
                        "CREATE " + index + " " + SALTED_TABLE_SALTED_INDEX_NAME + " ON " + SALTED_TABLE_NAME + " (col1) INCLUDE (col0)"
                        + (index.toUpperCase().startsWith("LOCAL") ? "" : " SALT_BUCKETS=4"));
                conn.commit();
            }
        } catch (TableAlreadyExistsException e) {
        }
        conn.close();        
    }
    
    protected static final String KEY_ORDERING_TABLE_1_NAME = "key_ordering_test_table_1";
    protected static final String KEY_ORDERING_TABLE_2_NAME = "key_ordering_test_table_2";
    
    protected void initKeyOrderingTable(String url) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + KEY_ORDERING_TABLE_1_NAME
                    + "(k0 BIGINT NOT NULL, k1 INTEGER NOT NULL, v0 INTEGER, v1 BIGINT CONSTRAINT pk PRIMARY KEY (k0 DESC, k1))");
            conn.createStatement().execute(
                    "CREATE TABLE " + KEY_ORDERING_TABLE_2_NAME
                    + "(k0 BIGINT NOT NULL, k1 BIGINT NOT NULL, v0 INTEGER, v1 BIGINT CONSTRAINT pk PRIMARY KEY (k0 DESC, k1 DESC))");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + KEY_ORDERING_TABLE_1_NAME
                    + " VALUES(?, ?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setInt(3, 1);
            stmt.setInt(4, 2);
            stmt.execute();
            stmt.setInt(1, 1);
            stmt.setInt(2, 3);
            stmt.setInt(3, 1);
            stmt.setInt(4, 3);
            stmt.execute();
            stmt.setInt(1, 1);
            stmt.setInt(2, 4);
            stmt.setInt(3, 1);
            stmt.setInt(4, 4);
            stmt.execute();
            stmt.setInt(1, 1);
            stmt.setInt(2, 5);
            stmt.setInt(3, 1);
            stmt.setInt(4, 5);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 3);
            stmt.setInt(3, 2);
            stmt.setInt(4, 3);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 5);
            stmt.setInt(3, 2);
            stmt.setInt(4, 5);
            stmt.execute();
            stmt.setInt(1, 3);
            stmt.setInt(2, 2);
            stmt.setInt(3, 3);
            stmt.setInt(4, 2);
            stmt.execute();
            stmt.setInt(1, 5);
            stmt.setInt(2, 2);
            stmt.setInt(3, 5);
            stmt.setInt(4, 2);
            stmt.execute();
            stmt.setInt(1, 5);
            stmt.setInt(2, 5);
            stmt.setInt(3, 5);
            stmt.setInt(4, 5);
            stmt.execute();
            conn.commit();
            stmt = conn.prepareStatement(
                    "UPSERT INTO " + KEY_ORDERING_TABLE_2_NAME
                    + " VALUES(?, ?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setInt(3, 1);
            stmt.setInt(4, 2);
            stmt.execute();
            stmt.setInt(1, 1);
            stmt.setInt(2, 5);
            stmt.setInt(3, 1);
            stmt.setInt(4, 5);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 2);
            stmt.setInt(3, 2);
            stmt.setInt(4, 2);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 3);
            stmt.setInt(3, 2);
            stmt.setInt(4, 3);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 4);
            stmt.setInt(3, 2);
            stmt.setInt(4, 4);
            stmt.execute();
            stmt.setInt(1, 2);
            stmt.setInt(2, 5);
            stmt.setInt(3, 2);
            stmt.setInt(4, 5);
            stmt.execute();
            stmt.setInt(1, 4);
            stmt.setInt(2, 3);
            stmt.setInt(3, 4);
            stmt.setInt(4, 3);
            stmt.execute();
            stmt.setInt(1, 5);
            stmt.setInt(2, 4);
            stmt.setInt(3, 5);
            stmt.setInt(4, 4);
            stmt.execute();
            stmt.setInt(1, 5);
            stmt.setInt(2, 5);
            stmt.setInt(3, 5);
            stmt.setInt(4, 5);
            stmt.execute();
            conn.commit();
        } catch (TableAlreadyExistsException e) {
        }
        conn.close();        
    }
    
    protected static final String MULTI_TENANT_TABLE = "multitenant_test_table";
    protected static final String MULTI_TENANT_TABLE_INDEX = "idx_multitenant_test_table";
    protected static final String MULTI_TENANT_VIEW1 = "s1.multitenant_test_view1";
    protected static final String MULTI_TENANT_VIEW1_INDEX = "idx_multitenant_test_view1";
    protected static final String MULTI_TENANT_VIEW2 = "s2.multitenant_test_view2";
    protected static final String MULTI_TENANT_VIEW2_INDEX = "idx_multitenant_test_view2";
    
    protected void initMultiTenantTables(String url, String index) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE " + MULTI_TENANT_TABLE + " (tenant_id VARCHAR NOT NULL, id VARCHAR NOT NULL, col0 INTEGER, col1 INTEGER, col2 INTEGER CONSTRAINT pk PRIMARY KEY (tenant_id, id)) MULTI_TENANT=true");
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + MULTI_TENANT_TABLE
                    + " VALUES(?, ?, ?, ?, ?)");
            DecimalFormat formatter = new DecimalFormat("0000");
            for (int i = 0; i < 1000; i++) {
                stmt.setString(1, "10");
                stmt.setString(2, formatter.format(2 + i));
                stmt.setInt(3, 3 + i);
                stmt.setInt(4, 4 + i);
                stmt.setInt(5, 5 + i);
                stmt.execute();
            }
            for (int i = 0; i < 1000; i++) {
                stmt.setString(1, "15");
                stmt.setString(2, formatter.format(3 + i));
                stmt.setInt(3, 4 + i);
                stmt.setInt(4, 5 + i);
                stmt.setInt(5, 6 + i);
                stmt.execute();
            }
            for (int i = 0; i < 1000; i++) {
                stmt.setString(1, "20");
                stmt.setString(2, formatter.format(4 + i));
                stmt.setInt(3, 5 + i);
                stmt.setInt(4, 6 + i);
                stmt.setInt(5, 7 + i);
                stmt.execute();
            }
            conn.commit();
            
            if (index != null) {
                conn.createStatement().execute(
                        "CREATE " + index + " " + MULTI_TENANT_TABLE_INDEX
                        + " ON " + MULTI_TENANT_TABLE + "(col1) INCLUDE (col0, col2)");
                conn.commit();
            }
            
            conn.close();
            props.setProperty("TenantId", "10");
            conn = DriverManager.getConnection(getOldUrl(), props);
            conn.createStatement().execute("CREATE VIEW " + MULTI_TENANT_VIEW1
                    + " AS select * from " + MULTI_TENANT_TABLE);
            conn.commit();
            
            if (index != null) {
                conn.createStatement().execute(
                        "CREATE " + index + " " + MULTI_TENANT_VIEW1_INDEX
                        + " ON " + MULTI_TENANT_VIEW1 + "(col0)");
                conn.commit();
            }
            
            conn.close();
            props.setProperty("TenantId", "20");
            conn = DriverManager.getConnection(getOldUrl(), props);
            conn.createStatement().execute("CREATE VIEW " + MULTI_TENANT_VIEW2
                    + " AS select * from " + MULTI_TENANT_TABLE + " where col2 > 7");
            conn.commit();
            
            if (index != null) {
                conn.createStatement().execute(
                        "CREATE " + index + " " + MULTI_TENANT_VIEW2_INDEX
                        + " ON " + MULTI_TENANT_VIEW2 + "(col0)");
                conn.commit();
            }
        } catch (TableAlreadyExistsException e) {
        } finally {
            conn.close();
        }
    }

}
