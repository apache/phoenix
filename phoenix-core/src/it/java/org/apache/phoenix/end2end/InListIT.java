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
package org.apache.phoenix.end2end;

import static java.util.Collections.singletonList;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.ExplainTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class InListIT extends ParallelStatsDisabledIT {

    private static class TestWhereExpressionCompiler extends ExpressionCompiler {
        private boolean disambiguateWithFamily;

        public TestWhereExpressionCompiler(StatementContext context) {
            super(context);
        }

        @Override
        public Expression visit(ColumnParseNode node) throws SQLException {
            ColumnRef ref = resolveColumn(node);
            TableRef tableRef = ref.getTableRef();
            Expression newColumnExpression = ref.newColumnExpression(node.isTableNameCaseSensitive(), node.isCaseSensitive());
            if (tableRef.equals(context.getCurrentTable()) && !SchemaUtil.isPKColumn(ref.getColumn())) {
                byte[] cq = tableRef.getTable().getImmutableStorageScheme() == PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
                    ? QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES : ref.getColumn().getColumnQualifierBytes();
                // track the where condition columns. Later we need to ensure the Scan in HRS scans these column CFs
                context.addWhereConditionColumn(ref.getColumn().getFamilyName().getBytes(), cq);
            }
            return newColumnExpression;
        }

        @Override
        protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
            ColumnRef ref = super.resolveColumn(node);
            if (disambiguateWithFamily) {
                return ref;
            }
            PTable table = ref.getTable();
            // Track if we need to compare KeyValue during filter evaluation
            // using column family. If the column qualifier is enough, we
            // just use that.
            if (!SchemaUtil.isPKColumn(ref.getColumn())) {
                if (!EncodedColumnsUtil.usesEncodedColumnNames(table)
                    || ref.getColumn().isDynamic()) {
                    try {
                        table.getColumnForColumnName(ref.getColumn().getName().getString());
                    } catch (AmbiguousColumnException e) {
                        disambiguateWithFamily = true;
                    }
                } else {
                    for (PColumnFamily columnFamily : table.getColumnFamilies()) {
                        if (columnFamily.getName().equals(ref.getColumn().getFamilyName())) {
                            continue;
                        }
                        try {
                            table.getColumnForColumnQualifier(columnFamily.getName().getBytes(),
                                ref.getColumn().getColumnQualifierBytes());
                            // If we find the same qualifier name with different columnFamily,
                            // then set disambiguateWithFamily to true
                            disambiguateWithFamily = true;
                            break;
                        } catch (ColumnNotFoundException ignore) {
                        }
                    }
                }
            }
            return ref;
        }
    }

    private static boolean isInitialized = false;
    private static String tableName = generateUniqueName();
    private static String tableName2 = generateUniqueName();
    private static String descViewName = generateUniqueName();
    private static String ascViewName = generateUniqueName();
    private static String viewName1 = generateUniqueName();
    private static String viewName2 = generateUniqueName();
    private static String prefix = generateUniqueName();

    boolean checkMaxSkipScanCardinality = true;
    boolean bloomFilter = true;

    public InListIT(boolean param1, boolean param2) throws Exception {
        // Setup max skip scan size appropriate for the tests.
        checkMaxSkipScanCardinality = param1;
        // Run tests with and with bloom filter
        bloomFilter = param2;
        Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
            put(QueryServices.MAX_IN_LIST_SKIP_SCAN_SIZE, checkMaxSkipScanCardinality ? String.valueOf(15) : String.valueOf(-1));
        }};
        ReadOnlyProps props = new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS, DEFAULT_PROPERTIES.entrySet().iterator());
        initAndRegisterTestDriver(getUrl(), props);

    }

    @Parameterized.Parameters(name="checkMaxSkipScanCardinality = {0}, bloomFilter = {1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {true, true},
            {true, false},
            {false, true},
            {false, false}
        });
    }

    @BeforeClass
    public static final void doSetup() throws Exception {

        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS, ReadOnlyProps.EMPTY_PROPS);
        TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant1";
        TENANT_URL = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + TENANT_ID;
    }

    @Before
    public void setup() throws Exception {
        if(isInitialized){
            return;
        }
        initializeTables();
        isInitialized = true;
    }

    @After
    public void cleanUp() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        deleteTenantData(descViewName);
        deleteTenantData(viewName1);
        deleteTenantData(viewName2);
        deleteTenantData(ascViewName);
        deleteTenantData(tableName);
        deleteTenantData(tableName2);
        assertFalse("refCount leaked", refCountLeaked);
    }

    @Test
    public void testLeadingPKWithTrailingRVC() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "( col1 VARCHAR NOT NULL,"
                + "  col2 VARCHAR NOT NULL, "
                + "  id VARCHAR NOT NULL,"
                + "  CONSTRAINT pk PRIMARY KEY (col1, col2, id))");

        conn.createStatement().execute("upsert into " + tableName + " (col1, col2, id) values ('a', 'b', 'c')");
        conn.createStatement().execute("upsert into " + tableName + " (col1, col2, id) values ('a', 'b', 'd')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from " + tableName + " WHERE col1 = 'a' and ((col2, id) IN (('b', 'c'),('b', 'e')))");
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    @Test
    public void testLeadingPKWithTrailingRVC2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( user VARCHAR, tenant_id VARCHAR(5) NOT NULL,tenant_type_id VARCHAR(3) NOT NULL,  id INTEGER NOT NULL CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id))");

        conn.createStatement().execute("upsert into " + tableName + " (tenant_id, tenant_type_id, id, user) values ('a', 'a', 1, 'BonA')");
        conn.createStatement().execute("upsert into " + tableName + " (tenant_id, tenant_type_id, id, user) values ('a', 'a', 2, 'BonB')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select id from " + tableName + " WHERE tenant_id = 'a' and tenant_type_id = 'a' and ((id, user) IN ((1, 'BonA'),(1, 'BonB')))");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
    }

    /**
     * Builds the DDL statement that will create a table with the given properties.
     * Assumes 5 pk columns of the given type.
     * Adds a non primary key column named "nonPk"
     * @param tableName  the name to use for the table
     * @param pkType  the data type of the primary key columns
     * @param saltBuckets  the number of salt buckets if the table is salted, otherwise 0
     * @param isMultiTenant  whether or not the table needs a tenant_id column
     * @return  the final DDL statement
     */
    private static String createTableDDL(String tableName, PDataType pkType, int saltBuckets,
            boolean isMultiTenant, boolean isBloomFilterEnabled) {
        StringBuilder ddlBuilder = new StringBuilder();
        ddlBuilder.append("CREATE TABLE ").append(tableName).append(" ( ");

        // column declarations
        if(isMultiTenant) {
            ddlBuilder.append("tenantId VARCHAR(5) NOT NULL, ");
        }
        for(int i = 0; i < 5; i++) {
            ddlBuilder.append("pk").append(i + 1).append(" ").append(pkType.getSqlTypeName()).append(" NOT NULL, ");
        }
        ddlBuilder.append("nonPk VARCHAR ");

        // primary key constraint declaration
        ddlBuilder.append("CONSTRAINT pk PRIMARY KEY (");
        if(isMultiTenant) {
            ddlBuilder.append("tenantId, ");
        }
        ddlBuilder.append("pk1, pk2, pk3, pk4, pk5) ) ");

        // modifier declarations
        if(saltBuckets != 0) {
            ddlBuilder.append("SALT_BUCKETS = ").append(saltBuckets);
        }
        if(saltBuckets != 0 && isMultiTenant) {
            ddlBuilder.append(", ");
        }
        if(isMultiTenant) {
            ddlBuilder.append("MULTI_TENANT=true");
        }
        if (isBloomFilterEnabled) {
            if (saltBuckets != 0 || isMultiTenant) {
                ddlBuilder.append(", ");
            }
            ddlBuilder.append("BLOOMFILTER='ROW'");
        }
        return ddlBuilder.toString();
    }

    /**
     * Creates a table with the given properties and returns its name. If the table is multi-tenant,
     * also creates a tenant view for that table and returns the name of the view instead.
     * @param baseConn  a non-tenant specific connection. Used to create the base tables
     * @param conn  a tenant-specific connection, if necessary. Otherwise ignored.
     * @param isMultiTenant  whether or not this table should be multi-tenant
     * @param pkType  the data type of the primary key columns
     * @param saltBuckets  the number of salt buckets if the table is salted, otherwise 0
     * @return  the table or view name that should be used to access the created table
     */
    private static String initializeAndGetTable(Connection baseConn, Connection conn,
            boolean isMultiTenant, PDataType pkType, int saltBuckets, boolean isBloomFilterEnabled)
            throws SQLException {
        String tableName = getTableName(isMultiTenant, pkType, saltBuckets);
        String tableDDL = createTableDDL(tableName, pkType, saltBuckets,
                isMultiTenant, isBloomFilterEnabled);
        baseConn.createStatement().execute(tableDDL);

        // if requested, create a tenant specific view and return the view name instead
        if(isMultiTenant) {
            String viewName = tableName + "_view";
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName;
            conn.createStatement().execute(viewDDL);
            return viewName;
        }
        else {
            return tableName;
        }
    }


    private String getType(PDataType pkType) {
        String pkTypeStr = "VARCHAR(25)";
        switch (pkType.getSqlType()) {
        case Types.VARCHAR:
            pkTypeStr = "VARCHAR(25)";
            break;
        case Types.CHAR:
            pkTypeStr = "CHAR(15)";
            break;
        case Types.DECIMAL:
            pkTypeStr = "DECIMAL(8,2)";
            break;
        case Types.INTEGER:
            pkTypeStr = "INTEGER";
            break;
        case Types.BIGINT:
            pkTypeStr = "BIGINT";
            break;
        case Types.DATE:
            pkTypeStr = "DATE";
            break;
        case Types.TIMESTAMP:
            pkTypeStr = "TIMESTAMP";
            break;
        default:
            pkTypeStr = "VARCHAR(25)";
        }
        return pkTypeStr;
    }

    private static void createBaseTable(String baseTable) throws SQLException {

        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            try (Statement cstmt = globalConnection.createStatement()) {
                String CO_BASE_TBL_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s(OID CHAR(15) NOT NULL,KP CHAR(3) NOT NULL,ROW_ID VARCHAR, COL1 VARCHAR,COL2 VARCHAR,COL3 VARCHAR,CREATED_DATE DATE,CREATED_BY CHAR(15),LAST_UPDATE DATE,LAST_UPDATE_BY CHAR(15),SYSTEM_MODSTAMP DATE CONSTRAINT pk PRIMARY KEY (OID,KP)) MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0";
                cstmt.execute(String.format(CO_BASE_TBL_TEMPLATE, baseTable));
            }
        }
        return;
    }

    private void dropTenantViewData(int tenant, String tenantView) throws SQLException {

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenant);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            tenantConnection.setAutoCommit(true);
            try (Statement cstmt = tenantConnection.createStatement()) {
                cstmt.execute(String.format("DELETE FROM %s", tenantView));
            }
        }
        return;
    }

    private void createTenantView(int tenant, String baseTable, String tenantView, String partition,
            PDataType pkType1, SortOrder pk1Order,
            PDataType pkType2, SortOrder pk2Order,
            PDataType pkType3, SortOrder pk3Order) throws SQLException {

        String pkType1Str = getType(pkType1);
        String pkType2Str = getType(pkType2);
        String pkType3Str = getType(pkType3);
        createBaseTable(baseTable);

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenant);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            try (Statement cstmt = tenantConnection.createStatement()) {
                String TENANT_VIEW_TEMPLATE = "CREATE VIEW IF NOT EXISTS %s(ID1 %s not null,ID2 %s not null,ID3 %s not null,COL4 VARCHAR,COL5 VARCHAR,COL6 VARCHAR CONSTRAINT pk PRIMARY KEY (ID1 %s, ID2 %s, ID3 %s)) "
                        + "AS SELECT * FROM %s WHERE KP = '%s'";
                cstmt.execute(String.format(TENANT_VIEW_TEMPLATE, tenantView, pkType1Str, pkType2Str, pkType3Str, pk1Order.name(),pk2Order.name(),pk3Order.name(), baseTable, partition));
            }
        }
        return;
    }

    private static String getTableName(boolean isMultiTenant, PDataType pkType, int saltBuckets) {
        return prefix+"init_in_test_" + pkType.getSqlTypeName() + saltBuckets + (isMultiTenant ?
            "_multi" :
            "_single");
    }

    private static String TENANT_SPECIFIC_URL1;
    private static final String  TENANT_PREFIX = "Txt00tst1";
    private static final String TENANT_ID = "ABC";
    private static String TENANT_URL;

    // the different combinations to check each test against
    private static final List<Boolean> TENANCIES = Arrays.asList(false, true);
    private static final List<? extends PDataType> INTEGER_TYPES = Arrays.asList(PInteger.INSTANCE);
    private static final List<Integer> SALT_BUCKET_NUMBERS = Arrays.asList(0, 4);

    private static final List<String> HINTS = Arrays.asList("/*+ SKIP_SCAN */", "/*+ RANGE_SCAN */");

    private  void initializeTables() throws Exception {
        buildSchema(tableName, viewName1, true);
        buildSchema(tableName2, viewName2, false);
        for (boolean isMultiTenant : TENANCIES) {
            Connection baseConn = DriverManager.getConnection(getUrl());
            Connection conn = isMultiTenant ? DriverManager.getConnection(TENANT_URL) : baseConn;

            try {
                // test each combination of types and salting
                for (PDataType pkType : INTEGER_TYPES) {
                    for (int saltBuckets : SALT_BUCKET_NUMBERS) {
                        // use a different table with a unique name for each variation
                        String tableName =
                            initializeAndGetTable(baseConn, conn, isMultiTenant, pkType,
                                saltBuckets, bloomFilter);

                        // upsert the given data
                        for (String upsertBody : DEFAULT_UPSERT_BODIES) {
                            conn.createStatement()
                                .execute("UPSERT INTO " + tableName + " " + upsertBody);
                        }
                        conn.commit();
                    }
                }
            }

            // clean up the connections used
            finally {
                baseConn.close();
                if (!conn.isClosed()) {
                    conn.close();
                }

            }
        }
    }

    /**
     * Tests the given where clause against the given upserts by comparing against the list of
     * expected result strings.
     * @param whereClause  the where clause to test. Should only refer to the pks upserted.
     * @param expecteds  a complete list of all of the expected result row names
     */
    private void testWithIntegerTypesWithVariedSaltingAndTenancy(String whereClause,
        List<String> expecteds) throws SQLException {
        // test single and multitenant tables
        for(boolean isMultiTenant : TENANCIES) {
            Connection baseConn = DriverManager.getConnection(getUrl());
            Connection conn = isMultiTenant ? DriverManager.getConnection(TENANT_URL)
                    : baseConn;

            try {
                // test each combination of types and salting
                for(PDataType pkType : INTEGER_TYPES) {
                    for(int saltBuckets : SALT_BUCKET_NUMBERS) {
                        // use a different table with a unique name for each variation
                        String tableName = getTableName(isMultiTenant, pkType, saltBuckets);

                        for(String hint : HINTS) {
                            String context = "where: " + whereClause + ", type: " + pkType + ", salt buckets: "
                                    + saltBuckets + ", multitenant: " + isMultiTenant + ", hint: " + hint + "";

                            // perform the query
                            String sql = "SELECT " + hint + " nonPk FROM " + tableName + " " + whereClause;
                            ResultSet rs = conn.createStatement().executeQuery(sql);
                            for (String expected : expecteds) {
                                assertTrue("did not include result '" + expected + "' (" + context + ")", rs.next());
                                assertEquals(context, expected, rs.getString(1));
                            }
                            assertFalse(context, rs.next());
                        }
                    }
                }
            }
            // clean up the connections used
            finally {
                baseConn.close();
                if(!conn.isClosed()) {
                    conn.close();
                }
            }
        }
    }

    List<List<Object>> DEFAULT_UPSERTS = Arrays.asList(Arrays.<Object>asList(1, 2, 4, 5, 6, "row1"),
            Arrays.<Object>asList(2, 3, 4, 5, 6, "row2"),
            Arrays.<Object>asList(2, 3, 6, 4, 5, "row3"),
            Arrays.<Object>asList(6, 5, 4, 3, 2, "row4"));

    List<String> DEFAULT_UPSERT_BODIES = Lists.transform(DEFAULT_UPSERTS, new Function<List<Object>, String>() {
        @Override
        public String apply(List<Object> input) {
            List<Object> pks = input.subList(0, 5);
            Object nonPk = input.get(5);

            return "(pk1, pk2, pk3, pk4, pk5, nonPk) VALUES ( "
                    + Joiner.on(", ").join(pks) + ", '" + nonPk + "')";
        }
    });

    // test variations used:
    // 1. queries with no results
    // 2. queries with fully qualified row keys
    // 3. queries with partiall qualified row keys, starting from the beginning
    // 4. queries with partially qualified row keys, but not the beginning
    // 5. queries with partially qualified row keys with a "hole slot" in the middle

    @Test
    public void testPlainRVCNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 3, 4, 5), (1, 2, 4, 5, 3))";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testPlainRVCFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 3, 4, 5), (1, 2, 4, 5, 6))";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testPlainRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4) IN ((2, 3, 4, 5), (1, 2, 4, 5))";
        List<String> expecteds = Arrays.asList("row1", "row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testPlainRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk2, pk3, pk4, pk5) IN ((2, 3, 4, 5), (2, 4, 5, 6))";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testPlainRVCSlotHole() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk4, pk5) IN ((1, 2, 4, 5), (6, 5, 3, 2))";
        List<String> expecteds = singletonList("row4");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCNoResults() throws Exception {
        String whereClause = "WHERE pk1 != 2 AND (pk3, pk4, pk5) IN ((6, 4, 5), (5, 6, 4))";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCFullyQualified() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk2, pk3, pk4, pk5) IN ((2, 4, 5, 6), (3, 4, 5, 6))";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk2, pk3) IN ((3, 6), (5, 4))";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE pk2 = 2 AND (pk3, pk4, pk5) IN ((4, 5, 6), (5, 6, 4))";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingPKWithTrailingRVCSlotHole() throws Exception {
        String whereClause = "WHERE pk1 = 2 AND (pk3, pk4, pk5) IN ((4, 5, 6), (5, 6, 4))";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk4 = 3";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4) IN ((1, 2, 4, 5), (2, 3, 4, 5)) AND pk5 = 6";
        List<String> expecteds = Arrays.asList("row1", "row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk4 = 4";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk2, pk3, pk4) IN ((3, 4, 5), (3, 6, 4)) AND pk5 = 5";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testLeadingRVCWithTrailingPKSlotHole() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 4), (2, 3, 6)) AND pk5 = 5";
        List<String> expecteds = singletonList("row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND pk2 = 4";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3, pk4, pk5) IN ((1, 2, 4, 5, 6), (2, 3, 4, 5, 6)) AND pk1 = 2";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((1, 2, 4), (2, 3, 6)) AND pk3 = 4";
        List<String> expecteds = singletonList("row1");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndPKPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk3, pk4, pk5) IN ((4, 5, 6), (4, 3, 2)) AND pk5 = 2";
        List<String> expecteds = singletonList("row4");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCNoResults() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND (pk2, pk3) IN ((4, 4), (4, 6))";
        List<String> expecteds = Collections.<String>emptyList();

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCFullyQualified() throws Exception {
        String whereClause = "WHERE (pk1, pk2, pk3) IN ((2, 3, 6), (2, 3, 4)) AND (pk3, pk4, pk5) IN ((4, 5, 6), (4, 3, 2))";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test public void testOverlappingRVCWithMiddleColumn() throws Exception {
        String whereClause =
            "WHERE pk2=3 and (pk1, pk2, pk3, pk4) IN ((2, 3, 6, 6), (2, 3, 4, 5)) ";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause,
            expecteds);
    }

    @Test public void testOverlappingRVCWithMultipleMiddleColumn() throws Exception {
        String whereClause =
            "WHERE (pk2,pk3) in ((3,4)) and (pk1, pk2, pk3, pk4) IN ((2, 3, 6, 6), (2, 3, 4, 5)) ";
        List<String> expecteds = singletonList("row2");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause,
            expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCPartiallyQualifiedBegin() throws Exception {
        String whereClause = "WHERE (pk1, pk2) IN ((1, 2), (2, 3)) AND (pk2, pk3) IN ((3, 4), (3, 6))";
        List<String> expecteds = Arrays.asList("row2", "row3");

        testWithIntegerTypesWithVariedSaltingAndTenancy( whereClause, expecteds);
    }

    @Test
    public void testOverlappingRVCAndRVCPartiallyQualifiedEnd() throws Exception {
        String whereClause = "WHERE (pk3, pk4) IN ((4, 5), (4, 3)) AND (pk4, pk5) IN ((3, 2), (4, 5))";
        List<String> expecteds = singletonList("row4");

        testWithIntegerTypesWithVariedSaltingAndTenancy(whereClause, expecteds);
    }

    @Test
    public void testWithFixedLengthDescPK() throws Exception {
        testWithFixedLengthPK(SortOrder.DESC);
    }

    @Test
    public void testWithFixedLengthAscPK() throws Exception {
        testWithFixedLengthPK(SortOrder.ASC);
    }

    @Test
    public void testWithFixedLengthKV() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( id INTEGER PRIMARY KEY, k CHAR(3))");

        conn.createStatement().execute("upsert into " + tableName + " values (1, 'aa')");
        conn.createStatement().execute("upsert into " + tableName + " values (2, 'bb')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select k from " + tableName + " WHERE k IN ('aa','bb')");
        assertTrue(rs.next());
        assertEquals("aa", rs.getString(1));
        assertTrue(rs.next());
        assertEquals("bb", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    private void testWithFixedLengthPK(SortOrder sortOrder) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( k CHAR(3) PRIMARY KEY " + (sortOrder == SortOrder.DESC ? "DESC" : "") + ")");

        conn.createStatement().execute("upsert into " + tableName + " (k) values ('aa')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('bb')");
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("select k from " + tableName + " WHERE k IN ('aa','bb')");
        assertTrue(rs.next());
        assertEquals(sortOrder == SortOrder.ASC ? "aa" : "bb", rs.getString(1));
        assertTrue(rs.next());
        assertEquals(sortOrder == SortOrder.ASC ? "bb" : "aa", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    @Test
    public void testInListExpressionWithNull() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN ('', 'FOO')");
            assertTrue(rs.next());
            int result1 = rs.getInt(1);
            assertEquals(0, result1);
            ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID = '' OR TENANT_ID = 'FOO'");
            assertTrue(rs2.next());
            assertEquals(result1, rs2.getInt(1));
        }
    }

    @Test
    public void testUpperWithInChar() throws Exception {
        String baseTable = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE  TABLE " + baseTable +
                    " (ID BIGINT NOT NULL primary key, A CHAR(2))");
            PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO  " + baseTable +
                    " VALUES (?, ?)");
            pstmt.setInt(1, 1);
            pstmt.setString(2, "a");
            pstmt.executeUpdate();
            conn.commit();
            pstmt.setInt(1, 2);
            pstmt.setString(2, "b");
            pstmt.executeUpdate();
            conn.commit();

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + baseTable  +
                    " WHERE UPPER(A) IN ('A', 'C')");
            assertTrue(rs.next());
            assertEquals(rs.getString(2), "a");
            assertFalse(rs.next());
        }
    }

    @Test
    public void testLowerWithInChar() throws Exception {
        String baseTable = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl());
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE  TABLE " + baseTable +
                    " (ID BIGINT NOT NULL primary key, A CHAR(2))");
            PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO  " + baseTable +
                    " VALUES (?, ?)");
            pstmt.setInt(1, 1);
            pstmt.setString(2, "A");
            pstmt.executeUpdate();
            conn.commit();
            pstmt.setInt(1, 2);
            pstmt.setString(2, "B");
            pstmt.executeUpdate();
            conn.commit();

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + baseTable  +
                    " WHERE LOWER(A) IN ('a', 'c')");
            assertTrue(rs.next());
            assertEquals(rs.getString(2), "A");
            assertFalse(rs.next());
        }
    }

    @Test(expected = TypeMismatchException.class)
    public void testInListExpressionWithNotValidElements() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN (4, 8)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test(expected = SQLException.class)
    public void testInListExpressionWithNoElements() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN ()");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    public void testInListExpressionWithNullAndWrongTypedData() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM SYSTEM.CATALOG WHERE " +
                    "TENANT_ID IN ('', 4)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    public void testInListExpressionWithDesc() throws Exception {
        String fullTableName = generateUniqueName();
        String fullViewName = generateUniqueName();
        String tenantView = generateUniqueName();
        // create base table and global view using global connection
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(\n" +
                    "    TENANT_ID CHAR(15) NOT NULL,\n" +
                    "    KEY_PREFIX CHAR(3) NOT NULL,\n" +
                    "    CREATED_DATE DATE,\n" +
                    "    CREATED_BY CHAR(15),\n" +
                    "    SYSTEM_MODSTAMP DATE\n" +
                    "    CONSTRAINT PK PRIMARY KEY (\n" +
                    "       TENANT_ID," +
                    "       KEY_PREFIX" +
                    ")) MULTI_TENANT=TRUE");

            stmt.execute("CREATE VIEW " + fullViewName + "(\n" +
                    "    MODEL VARCHAR NOT NULL,\n" +
                    "    MILEAGE  BIGINT NOT NULL,\n" +
                    "    MILES_DRIVEN BIGINT NOT NULL,\n" +
                    "    MAKE VARCHAR,\n" +
                    "    CONSTRAINT PKVIEW PRIMARY KEY\n" +
                    "    (\n" +
                    "    MODEL, MILEAGE DESC, MILES_DRIVEN\n" +
                    ")) AS SELECT * FROM " + fullTableName + " WHERE KEY_PREFIX = '0CY'");

        }

        // create and use a tenant specific view to write data
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            Statement stmt = viewConn.createStatement();
            stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + fullViewName );
            viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(CREATED_BY, CREATED_DATE, SYSTEM_MODSTAMP, MODEL, MILEAGE, MILES_DRIVEN, MAKE) VALUES ('005xx000001Sv6o', 1532458254819, 1532458254819, 'a5', 23, 10000, 'AUDI')");
            viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(CREATED_BY, CREATED_DATE, SYSTEM_MODSTAMP, MODEL, MILEAGE, MILES_DRIVEN, MAKE) VALUES ('005xx000001Sv6o', 1532458254819, 1532458254819, 'a4', 27, 30000, 'AUDI')");
            viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(CREATED_BY, CREATED_DATE, SYSTEM_MODSTAMP, MODEL, MILEAGE, MILES_DRIVEN, MAKE) VALUES ('005xx000001Sv6o', 1532458254819, 1532458254819, '328i', 32, 40000, 'BMW')");
            viewConn.commit();

            ResultSet rs = stmt.executeQuery("SELECT Make, Model FROM " + tenantView + " WHERE MILEAGE IN (32, 27)");
            assertTrue(rs.next());
            assertEquals("BMW", rs.getString(1));
            assertEquals("328i", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("AUDI", rs.getString(1));
            assertEquals("a4", rs.getString(2));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("SELECT Make, Model FROM " + tenantView + " WHERE MILES_DRIVEN IN (30000, 40000)");
            assertTrue(rs.next());
            assertEquals("BMW", rs.getString(1));
            assertEquals("328i", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("AUDI", rs.getString(1));
            assertEquals("a4", rs.getString(2));
            assertFalse(rs.next());
            
            viewConn.createStatement().execute("DELETE FROM  " + tenantView + " WHERE MILEAGE IN (27, 32)");
            viewConn.commit();
            rs = stmt.executeQuery("SELECT Make, Model FROM " + tenantView);
            assertTrue(rs.next());
            assertEquals("AUDI", rs.getString(1));
            assertEquals("a5", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    private void buildSchema(String fullTableName, String fullViewName, boolean isDecOrder) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + fullTableName + "(\n" + " TENANT_ID CHAR(15) NOT NULL,\n" + " KEY_PREFIX CHAR(3) NOT NULL, ID5 BIGINT \n" +
                        " CONSTRAINT PK PRIMARY KEY (\n" + " TENANT_ID," + " KEY_PREFIX" + ")) MULTI_TENANT=TRUE");
                if (isDecOrder) {
                    stmt.execute("CREATE VIEW " + fullViewName + "(\n" + " ID1 VARCHAR NOT NULL,\n" + " ID2 VARCHAR NOT NULL,\n" + " ID3 BIGINT, ID4 BIGINT \n" +
                            " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (\n" + " ID1, ID2 DESC\n" + ")) " +
                            "AS SELECT * FROM " + fullTableName + " WHERE KEY_PREFIX = '0CY'");
                    try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
                        viewConn.setAutoCommit(true);
                        try (Statement tenantStmt = viewConn.createStatement()) {
                            tenantStmt.execute("CREATE VIEW IF NOT EXISTS " + this.descViewName + " AS SELECT * FROM " + fullViewName);
                        }
                    }
                } else {
                    stmt.execute("CREATE VIEW " + fullViewName + "(\n" + " ID1 VARCHAR NOT NULL,\n" + " ID2 VARCHAR NOT NULL,\n" + " ID3 BIGINT, ID4 BIGINT \n" +
                            " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                            "AS SELECT * FROM " + fullTableName + " WHERE KEY_PREFIX = '0CY'");
                    try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
                        viewConn.setAutoCommit(true);
                        try (Statement tenantStmt = viewConn.createStatement()) {
                            tenantStmt.execute("CREATE VIEW IF NOT EXISTS " + this.ascViewName + " AS SELECT * FROM " + fullViewName);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testPkDescOrderedTenantViewOnGlobalViewWithRightQueryPlan() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + descViewName);
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('foo', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000500')");
                viewConn.commit();

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID1, ID2) " +
                                "IN (('005xx000001Sv6o', '000000000000500'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    ExplainPlan plan = queryPlan.getExplainPlan();
                    ExplainPlanAttributes explainPlanAttributes =
                        plan.getPlanStepsAsAttributes();
                    assertTrue(explainPlanAttributes.getExplainScanType()
                        .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID2, ID1) " +
                                "IN (('000000000000500', '005xx000001Sv6o'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    ExplainPlan plan = queryPlan.getExplainPlan();
                    ExplainPlanAttributes explainPlanAttributes =
                        plan.getPlanStepsAsAttributes();
                    assertTrue(explainPlanAttributes.getExplainScanType()
                        .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('bar', '005xx000001Sv6o'))");

                ResultSet rs = stmt.executeQuery("SELECT ID2 FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals("000000000000500", rs.getString(1));
                stmt.execute("DELETE FROM " + tenantView);
            }
        }
    }

    @Test
    public void testColumnDescOrderedTenantViewOnGlobalViewWithStringValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + descViewName);
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('foo', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('bar', '000000000000400')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000500')");
                viewConn.commit();

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID1, ID2) " +
                                "IN (('005xx000001Sv6o', '000000000000500')," +
                                "('bar', '000000000000400')," +
                                "('foo', '000000000000300'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    ExplainPlan plan = queryPlan.getExplainPlan();
                    ExplainPlanAttributes explainPlanAttributes =
                        plan.getPlanStepsAsAttributes();
                    assertTrue(explainPlanAttributes.getExplainScanType()
                        .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                                "(('bar', '005xx000001Sv6o')," +
                                "('foo', '005xx000001Sv6o'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    ExplainPlan plan = queryPlan.getExplainPlan();
                    ExplainPlanAttributes explainPlanAttributes =
                        plan.getPlanStepsAsAttributes();
                    assertTrue(explainPlanAttributes.getExplainScanType()
                        .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', 'bar')," +
                        "('000000000000300', 'foo'))");

                ResultSet rs = stmt.executeQuery("SELECT ID2 FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals("000000000000500", rs.getString(1));

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                stmt.execute("DELETE FROM " + tenantView);
            }
        }
    }

    @Test
    public void testInListExpressionWithRightQueryPlanForTenantViewOnGlobalView() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + ascViewName);

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000400')");
                viewConn.commit();

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID1, ID2) IN " +
                                "(('005xx000001Sv6o', '000000000000500')," +
                                "('005xx000001Sv6o', '000000000000400')," +
                                "('005xx000001Sv6o', '000000000000300'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    ExplainPlan plan = queryPlan.getExplainPlan();
                    ExplainPlanAttributes explainPlanAttributes =
                        plan.getPlanStepsAsAttributes();
                    assertTrue(explainPlanAttributes.getExplainScanType()
                        .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
                }

                try (PreparedStatement preparedStmt = viewConn.prepareStatement(
                        "SELECT * FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                                "(('000000000000400', '005xx000001Sv6o')," +
                                "('000000000000300', '005xx000001Sv6o'))")) {
                    QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                    ExplainPlan plan = queryPlan.getExplainPlan();
                    ExplainPlanAttributes explainPlanAttributes =
                        plan.getPlanStepsAsAttributes();
                    assertTrue(explainPlanAttributes.getExplainScanType()
                        .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
                }
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', '005xx000001Sv6o')," +
                        "('000000000000300', '005xx000001Sv6o'))");
                assertTrue(rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000300', '005xx000001Sv6o'))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionGeneratesRightScanForAsc() throws Exception {
        testFullPkListPlan(this.ascViewName);
        testPartialPkListPlan(this.ascViewName);
        testPartialPkPlusNonPkListPlan(this.ascViewName);
        testNonPkListPlan(this.ascViewName);
    }

    @Test
    public void testInListExpressionGeneratesRightScanForDesc() throws Exception {
        testFullPkListPlan(this.descViewName);
        testPartialPkListPlan(this.descViewName);
        testPartialPkPlusNonPkListPlan(this.descViewName);
        testNonPkListPlan(this.descViewName);
    }

    private void testFullPkListPlan(String tenantView) throws Exception {
        Long numberOfRowsToScan = new Long(2);
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID1, ID2) IN " +
                    "(('005xx000001Sv6o', '000000000000500')," +
                    "('005xx000001Sv6o', '000000000000400'))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertEquals(numberOfRowsToScan, queryPlan.getEstimatedRowsToScan());
            ExplainPlan plan = queryPlan.getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertTrue(explainPlanAttributes.getExplainScanType()
                .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID1, ID2) IN " +
                    "(('005xx000001Sv6o', '000000000000500')," +
                    "('005xx000001Sv6o', '000000000000400'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertEquals(numberOfRowsToScan, queryPlan.getEstimatedRowsToScan());
            plan = queryPlan.getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertTrue(explainPlanAttributes.getExplainScanType()
                .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
        }
    }

    private void testPartialPkListPlan(String tenantView) throws Exception {
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID1) IN " +
                    "(('005xx000001Sv6o')," +
                    "('005xx000001Sv6o'))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            ExplainPlan plan = queryPlan.getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID1) IN " +
                    "(('005xx000001Sv6o')," +
                    "('005xx000001Sv6o'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID2) IN " +
                    "(('000000000000500')," +
                    "('000000000000400'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            plan = queryPlan.getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID2) IN " +
                    "(('000000000000500')," +
                    "('000000000000400'))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
        }
    }

    private void testPartialPkPlusNonPkListPlan(String tenantView) throws Exception {
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID1, ID3) IN " +
                    "(('005xx000001Sv6o', 1)," +
                    "('005xx000001Sv6o', 2))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            ExplainPlan plan = queryPlan.getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID1, ID3) IN " +
                    "(('005xx000001Sv6o', 1)," +
                    "('005xx000001Sv6o', 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));

            preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID2, ID3) IN " +
                    "(('000000000000500', 1)," +
                    "('000000000000400', 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            plan = queryPlan.getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID2, ID3) IN " +
                    "(('000000000000500', 1)," +
                    "('000000000000400', 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
        }
    }

    private void testNonPkListPlan(String tenantView) throws Exception {
        // Tenant connection should generate a range scan because tenant id is the leading PK.
        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1) ) {
            PreparedStatement preparedStmt = viewConn.prepareStatement("SELECT * FROM " + tenantView + " WHERE (ID3, ID4) IN " +
                    "((1, 1)," +
                    "(2, 2))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            ExplainPlan plan = queryPlan.getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());

            viewConn.prepareStatement("DELETE FROM " + tenantView + " WHERE (ID3, ID4) IN " +
                    "((1, 1)," +
                    "(2, 2))");
            queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            assertTrue(queryPlan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
        }
    }

    private void deleteTenantData(String tenantView) throws SQLException {
        try (Connection tenantConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            tenantConn.createStatement().execute("DELETE FROM " + tenantView);
        }
    }

    @Test
    public void testInListExpressionWithRightQueryPlanForNumericalValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(DOUBLE1 DOUBLE NOT NULL, INT1 BIGINT NOT NULL " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (DOUBLE1, INT1)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "(12.0, 8)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "(13.0, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "((8, 12.0)," +
                        "(9, 13.0))");
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

                stmt.execute("DELETE FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "((8, 12.0)," +
                        "(9, 13.0))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testQueryPlanForPkDescOrderedTenantViewOnGlobalViewForStringValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + " AS SELECT * FROM " + descViewName);
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('foo', '000000000000300')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('bar', '000000000000400')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2) VALUES " +
                        "('005xx000001Sv6o', '000000000000500')");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', 'bar')," +
                        "('000000000000300','foo'))");

                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "(('000000000000400', 'bar')," +
                        "('000000000000300','foo'))");

                rs = stmt.executeQuery("SELECT ID2 FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals("000000000000500", rs.getString(1));

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                stmt.execute("DELETE FROM " + tenantView);
            }
        }
    }

    @Test
    public void testQueryPlanForTenantViewOnBaseTableWithVarcharValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(DOUBLE1 VARCHAR NOT NULL, INT1 VARCHAR NOT NULL " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (DOUBLE1, INT1)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "('12.0', '8')");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(DOUBLE1, INT1) VALUES " +
                        "('13.0', '9')");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "(('8', '12.0')," +
                        "('9', '13.0'))");
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));

                stmt.execute("DELETE FROM " + tenantView + " WHERE (INT1, DOUBLE1) IN " +
                        "(('8', '12.0')," +
                        "('9', '13.0'))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testQueryPlanForTenantViewOnBaseTableWithNumericalValue() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView +
                        " (ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY " + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView +
                        " WHERE (ID1, ID5) IN " +
                        "((12, 7)," +
                        "(12, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID4, ID1) IN " +
                        "((9, 13)," +
                        "(12, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "((8, 12)," +
                        "(9, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2, ID1) IN " +
                        "((8, 12)," +
                        "(9, 13))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithFunction() throws Exception {
        String tenantView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID1 + 1, ID5) IN " +
                        "((13, 7)," +
                        "(13, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID4 - 1, ID1) IN " +
                        "((8, 13)," +
                        "(11, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2 + 1 , ID1 - 1) IN " +
                        "((9, 11)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2 -1, ID1 + 1) IN " +
                        "((7, 13)," +
                        "(8, 14))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithFunctionAndIndex() throws Exception {
        String tenantView = generateUniqueName();
        String tenantIndexView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                stmt.execute("CREATE INDEX " + tenantIndexView + " ON " + tenantView + " (ID5) INCLUDE (ID4, ID1)");

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID1 + 1, ID5) IN " +
                        "((13, 7)," +
                        "(13, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5 - 1, ID1) IN " +
                        "((6, 13)," +
                        "(12, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5 + 1 , ID1 - 1) IN " +
                        "((8, 11)," +
                        "(14, 12))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID5 -1, ID1 + 1) IN " +
                        "((6, 13)," +
                        "(12, 14))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithIndex() throws Exception {
        String tenantView = generateUniqueName();
        String tenantIndexView = generateUniqueName();

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                stmt.execute("CREATE INDEX " + tenantIndexView + " ON " + tenantView + " (ID5) INCLUDE (ID4)");

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5, ID1) IN " +
                        "((7, 12)," +
                        "(7, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2, ID5) IN " +
                        "((8, 13)," +
                        "(9, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID5, ID4) IN " +
                        "((7, 6)," +
                        "(13, 9))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID1, ID5) IN " +
                        "((12, 7)," +
                        "(13, 13))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testInListExpressionWithGlobalViewAndFunction() throws Exception {
        String tenantView = generateUniqueName();
        String globalView = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + globalView + " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");
            }
        }

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + globalView);

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(12, 8, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(13, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID1 + 1, ID5) IN " +
                        "((13, 7)," +
                        "(13, 13))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID4 - 1, ID1) IN " +
                        "((8, 13)," +
                        "(11, 13))");
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (ID2 + 1 , ID1 - 1) IN " +
                        "((9, 11)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1));
                assertEquals(8, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1));
                assertEquals(9, rs.getInt(2));
                assertEquals(13, rs.getInt(3));
                assertEquals(9, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (ID2 -1, ID1 + 1) IN " +
                        "((7, 13)," +
                        "(8, 14))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testGlobalViewWithPowerFunction() throws Exception {
        String tenantView = generateUniqueName();
        String globalView = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + globalView + " AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");
            }
        }

        try (Connection viewConn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            viewConn.setAutoCommit(true);
            try (Statement stmt = viewConn.createStatement()) {
                stmt.execute("CREATE VIEW IF NOT EXISTS " + tenantView + "(ID1 DOUBLE NOT NULL, ID2 DOUBLE NOT NULL, ID4 BIGINT " +
                        " CONSTRAINT PKVIEW PRIMARY KEY\n" + " (ID1, ID2)) " +
                        " AS SELECT * FROM " + globalView);

                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(9, 2, 7, 6)");
                viewConn.createStatement().execute("UPSERT INTO " + tenantView + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(2, 9, 13, 9)");
                viewConn.commit();

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + tenantView + " WHERE (POWER(ID2, 2), ID1) IN " +
                        "((4.0, 9)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + tenantView + " WHERE (POWER(ID1,2), ID2) IN " +
                        "((81, 2)," +
                        "(4, 9))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tenantView);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testBaseTableAndIndexTableHaveReversePKOrder() throws Exception {
        String view = generateUniqueName();
        String index = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + view + " (ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        "CONSTRAINT PKVIEW PRIMARY KEY (ID1, ID2)) AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(9, 2, 7, 6)");
                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(2, 9, 13, 9)");
                conn.commit();

                stmt.execute("CREATE INDEX " + index + " ON " + view + " (ID2, ID1) INCLUDE (ID5, ID4)");

                // TESTING for optimized scan
                PreparedStatement preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID1, ID2) IN " +
                        "((1, 1)," +
                        "(2, 2))");
                QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                ExplainPlan plan = queryPlan.getExplainPlan();
                ExplainPlanAttributes explainPlanAttributes =
                    plan.getPlanStepsAsAttributes();
                assertTrue(explainPlanAttributes.getExplainScanType()
                    .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));

                preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                plan = queryPlan.getExplainPlan();
                explainPlanAttributes = plan.getPlanStepsAsAttributes();
                assertTrue(explainPlanAttributes.getExplainScanType()
                    .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));

                preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                plan = queryPlan.getExplainPlan();
                explainPlanAttributes = plan.getPlanStepsAsAttributes();
                assertTrue(explainPlanAttributes.getExplainScanType()
                    .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));


                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + view + " WHERE (POWER(ID2, 2), ID1) IN " +
                        "((4.0, 9)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + view + " WHERE (POWER(ID1,2), ID2) IN " +
                        "((81, 2)," +
                        "(4, 9))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + view);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testDeletionFromTenantViewAndViewIndex() throws Exception {
        String view = generateUniqueName();
        String index = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(TENANT_SPECIFIC_URL1)) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE VIEW " + view + " (ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, ID4 BIGINT " +
                        "CONSTRAINT PKVIEW PRIMARY KEY (ID1, ID2)) AS SELECT * FROM " + tableName + " WHERE KEY_PREFIX = 'ABC'");

                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(9, 2, 7, 6)");
                conn.createStatement().execute("UPSERT INTO " + view + "(ID1, ID2, ID5, ID4) VALUES " +
                        "(2, 9, 13, 9)");
                conn.commit();

                stmt.execute("CREATE INDEX " + index + " ON " + view + " (ID4, ID2) INCLUDE (ID1, ID5)");

                // TESTING for optimized scan
                PreparedStatement preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID4, ID2) IN " +
                        "((1, 1)," +
                        "(2, 2))");
                QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                ExplainPlan plan = queryPlan.getExplainPlan();
                ExplainPlanAttributes explainPlanAttributes =
                    plan.getPlanStepsAsAttributes();
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());

                preparedStmt = conn.prepareStatement("SELECT ID1,ID5 FROM " + view + " WHERE (ID1, ID2) IN " +
                        "((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                plan = queryPlan.getExplainPlan();
                explainPlanAttributes = plan.getPlanStepsAsAttributes();
                assertTrue(explainPlanAttributes.getExplainScanType()
                    .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));


                preparedStmt = conn.prepareStatement("SELECT * FROM " + view + " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
                queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
                assertEquals(new Long(2), queryPlan.getEstimatedRowsToScan());
                plan = queryPlan.getExplainPlan();
                explainPlanAttributes = plan.getPlanStepsAsAttributes();
                assertTrue(explainPlanAttributes.getExplainScanType()
                    .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));

                ResultSet rs = stmt.executeQuery("SELECT ID1, ID2, ID5, ID4 FROM " + view + " WHERE (POWER(ID2, 2), ID1) IN " +
                        "((4.0, 9)," +
                        "(10, 12))");
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1));
                assertEquals(2, rs.getInt(2));
                assertEquals(7, rs.getInt(3));
                assertEquals(6, rs.getInt(4));
                assertTrue(!rs.next());

                stmt.execute("DELETE FROM " + view + " WHERE (POWER(ID1,2), ID2) IN " +
                        "((81, 2)," +
                        "(4, 9))");

                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + view);
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testBaseTableAndIndexTableHaveRightScan() throws Exception {
        String index = generateUniqueName();
        String fullTableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + fullTableName + "(ID1 BIGINT NOT NULL, ID2 BIGINT NOT NULL, " +
                        "VAL1 BIGINT, VAL2 BIGINT CONSTRAINT PK PRIMARY KEY (ID1,ID2))");
                stmt.execute("CREATE INDEX " + index + " ON " + fullTableName + " (ID2, ID1) INCLUDE (VAL2)");
            }

            PreparedStatement preparedStmt = conn.prepareStatement("SELECT VAL2 FROM " + fullTableName +
                    " WHERE (ID2, ID1) IN ((1, 1),(2, 2))");
            QueryPlan queryPlan = PhoenixRuntime.getOptimizedQueryPlan(preparedStmt);
            queryPlan.getTableRef().getTable().getType();
            ExplainPlan plan = queryPlan.getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertTrue(explainPlanAttributes.getExplainScanType()
                .startsWith(ExplainTable.POINT_LOOKUP_ON_STRING));
        }
    }

    @Test
    public void testInListExpressionWithVariableLengthColumnsRanges() throws Exception {
        Properties props = new Properties();
        final String schemaName = generateUniqueName();
        final String tableName = generateUniqueName();
        final String dataTableFullName = SchemaUtil.getTableName(schemaName, tableName);
        String ddl =
                "CREATE TABLE " + dataTableFullName + " (a VARCHAR(22) NOT NULL," +
                        "b CHAR(6) NOT NULL," +
                        "c VARCHAR(12) NOT NULL,d VARCHAR(200) NOT NULL, " +
                        "CONSTRAINT PK_TEST_KO PRIMARY KEY (a,b,c,d)) ";
        long startTS = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.commit();
            conn.createStatement().execute("upsert into "+ dataTableFullName+
                    " values('AAAA1234567890','202001','A1','foo')");
            conn.createStatement().execute("upsert into "+ dataTableFullName+
                    " values('AAAA1234567892','202002','A1','foo')");
            conn.createStatement().execute("upsert into "+ dataTableFullName+
                    " values('AAAA1234567892','202002','B1','foo')");
            conn.createStatement().execute("upsert into "+ dataTableFullName+
                    " values('AAAA1234567890','202001','B1','foo')");
            conn.commit();
            String query = "SELECT count(*) FROM "+dataTableFullName+
                    " WHERE (a, b) IN (('AAAA1234567890', '202001'), ( 'AAAA1234567892', '202002'))" +
                    " AND c IN ('A1')";
            ResultSet r  = conn.createStatement().executeQuery(query);
            r.next();
            assertEquals(2, r.getInt(1));
        }
    }

    @Test
    public void testWithVariousPKTypes() throws Exception {

        SortOrder[][] sortOrders = new SortOrder[][] {
                {SortOrder.ASC, SortOrder.ASC, SortOrder.ASC},
                {SortOrder.ASC, SortOrder.ASC, SortOrder.DESC},
                {SortOrder.ASC, SortOrder.DESC, SortOrder.ASC},
                {SortOrder.ASC, SortOrder.DESC, SortOrder.DESC},
                {SortOrder.DESC, SortOrder.ASC, SortOrder.ASC},
                {SortOrder.DESC, SortOrder.ASC, SortOrder.DESC},
                {SortOrder.DESC, SortOrder.DESC, SortOrder.ASC},
                {SortOrder.DESC, SortOrder.DESC, SortOrder.DESC}
        };

        PDataType[] testTSVarVarPKTypes = new PDataType[] { PTimestamp.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE};
        String baseTableName = generateUniqueName();
        String viewName = String.format("Z_%s", baseTableName);
        int tenantId = 1;
        int numTestCases = 3;

        for (int index=0;index<sortOrders.length;index++) {

            // Test Case 1: PK1 = Timestamp, PK2 = Varchar, PK3 = Varchar
            String view1Name = String.format("TEST_ENTITY.%s%d", viewName, index*numTestCases+1);
            String partition1 = String.format("Z%d", index*numTestCases+1);
            createTenantView(tenantId,
                    baseTableName, view1Name, partition1,
                    testTSVarVarPKTypes[0], sortOrders[index][0],
                    testTSVarVarPKTypes[1], sortOrders[index][1],
                    testTSVarVarPKTypes[2], sortOrders[index][2]);
            testTSVarVarPKs(tenantId, view1Name, sortOrders[index]);

            // Test Case 2: PK1 = Varchar, PK2 = Varchar, PK3 = Varchar
            String view2Name = String.format("TEST_ENTITY.%s%d", viewName, index*numTestCases+2);
            String partition2 = String.format("Z%d", index*numTestCases+2);
            PDataType[] testVarVarVarPKTypes = new PDataType[] { PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE};
            createTenantView(tenantId,
                    baseTableName, view2Name, partition2,
                    testVarVarVarPKTypes[0], sortOrders[index][0],
                    testVarVarVarPKTypes[1], sortOrders[index][1],
                    testVarVarVarPKTypes[2], sortOrders[index][2]);
            testVarVarVarPKs(tenantId, view2Name, sortOrders[index]);


            // Test Case 3: PK1 = Bigint, PK2 = Decimal, PK3 = Bigint
            String view3Name = String.format("TEST_ENTITY.%s%d", viewName, index*numTestCases+3);
            String partition3 = String.format("Z%d", index*numTestCases+3);
            PDataType[] testIntDecIntPKTypes = new PDataType[] { PLong.INSTANCE, PDecimal.INSTANCE, PLong.INSTANCE};
            createTenantView(tenantId,
                    baseTableName, view3Name, partition3,
                    testIntDecIntPKTypes[0], sortOrders[index][0],
                    testIntDecIntPKTypes[1], sortOrders[index][1],
                    testIntDecIntPKTypes[2], sortOrders[index][2]);
            testIntDecIntPK(tenantId, view3Name, sortOrders[index]);
        }
    }

    // Test Case 1: PK1 = Timestamp, PK2 = Varchar, PK3 = Varchar
    private void testTSVarVarPKs(int tenantId, String viewName, SortOrder[] sortOrder) throws SQLException {
        String testName = "testTSVarVarPKs";

        long nowTime = System.currentTimeMillis();
        List<String> UPSERT_SQLS = Arrays.asList(new String[] {
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime, "1", "5", "row01"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime, "1", "2", "row02"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime, "3", "5", "row03"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime, "3", "2", "row04"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+1, "2", "3", "row11"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+1, "2", "4", "row12"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+1, "2", "2", "row13"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+2, "4", "5", "row21"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+2, "1", "5", "row22"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+2, "1", "2", "row23"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+2, "3", "5", "row24"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+2, "3", "2", "row25"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+3, "1", "5", "row31"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+3, "1", "2", "row32"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+3, "3", "5", "row33"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+3, "3", "2", "row34"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, '%s', '%s', '%s')", viewName, nowTime+3, "6", "7", "row35")
        });
        String testSQL1 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, '21', '1'),(%d, '2', '31'))", viewName, nowTime, nowTime);
        String testSQL2 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, '4', '5'),(%d, '2', '3'))", viewName, nowTime+1, nowTime+1);
        String testMaxInList = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, '4', '5'),(%d, '2', '3')", viewName, nowTime+1, nowTime+1);

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            tenantConnection.setAutoCommit(true);
            try (Statement ustmt = tenantConnection.createStatement()) {
                for (String upsertSql : UPSERT_SQLS) {
                    ustmt.execute(upsertSql);
                }
            }
        }


        Set<String> expecteds1 = Collections.<String>emptySet();
        Set<String> expecteds2 = Sets.newHashSet(new String[] {"row11"});
        Set<String> expecteds3 = Sets.newHashSet(new String[] {"row11"});

        assertExpectedWithWhere(tenantId, testName, testSQL1, expecteds1);
        assertExpectedWithWhere(tenantId, testName, testSQL2, expecteds2);
        PDataType[] testPKTypes = new PDataType[] { PTimestamp.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE};
        assertExpectedWithMaxInList(tenantId, testName, testPKTypes, testMaxInList, sortOrder, expecteds3 );
    }

    // Test Case 2: PK1 = Varchar, PK2 = Varchar, PK3 = Varchar
    private void testVarVarVarPKs(int tenantId, String viewName, SortOrder[] sortOrder) throws SQLException {
        String testName = "testVarVarVarPKs";
        long nowTime = System.currentTimeMillis();
        List<String> UPSERT_SQLS = Arrays.asList(new String[] {
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime, 1, 5, "row01"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime, 1, 2, "row02"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime, 3, 5, "row03"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime, 3, 2, "row04"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+1, 2, 3, "row11"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+1, 2, 4, "row12"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+1, 2, 2, "row13"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+1, 1, 1, "row14"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+1, 1, 2, "row15"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 4, 5, "row21"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 1, 5, "row22"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 1, 2, "row23"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 3, 6, "row24"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 3, 4, "row25"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 5, 6, "row26"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+2, 5, 4, "row27"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+3, 6, 7, "row3"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+3, 2, 5, "row4"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES ('%s', '%s', '%s', '%s')", viewName, nowTime+3, 5, 3, "row5"),
                });
        String testSQL1 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN (('%s', '21', '1'),('%s', '2', '31'))", viewName, nowTime, nowTime);
        String testSQL2 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN (('%s', '4', '5'),('%s', '2', '3'))", viewName, nowTime+2, nowTime+1);
        String testMaxInList = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN (('%s', '4', '5'),('%s', '2', '3')", viewName, nowTime+2, nowTime+1);

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            tenantConnection.setAutoCommit(true);
            try (Statement ustmt = tenantConnection.createStatement()) {
                for (String upsertSql : UPSERT_SQLS) {
                    ustmt.execute(upsertSql);
                }
            }
        }


        Set<String> expecteds1 = Collections.<String>emptySet();
        Set<String> expecteds2 = Sets.newHashSet(new String[] {"row21", "row11"});
        Set<String> expecteds3 = Sets.newHashSet(new String[] {"row21", "row11"});

        assertExpectedWithWhere(tenantId, testName, testSQL1, expecteds1);
        assertExpectedWithWhere(tenantId, testName, testSQL2, expecteds2);
        PDataType[] testPKTypes = new PDataType[] { PVarchar.INSTANCE, PVarchar.INSTANCE, PVarchar.INSTANCE};
        assertExpectedWithMaxInList(tenantId, testName, testPKTypes, testMaxInList, sortOrder, expecteds3);

    }

    private void testIntDecIntPK(int tenantId, String viewName, SortOrder[] sortOrder) throws SQLException {
        String testName = "testIntDecIntPK";
        long nowTime = System.currentTimeMillis();
        List<String> UPSERT_SQLS = Arrays.asList(new String[] {
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, %f, %d, '%s')", viewName, nowTime, 2.0, 3, "row0"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, %f, %d, '%s')", viewName, nowTime+1, 2.0, 3, "row1"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, %f, %d, '%s')", viewName, nowTime+1, 2.0, 5, "row2"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, %f, %d, '%s')", viewName, nowTime+2, 4.0, 5, "row3"),
                String.format("UPSERT INTO %s(ID1, ID2, ID3, ROW_ID) VALUES (%d, %f, %d, '%s')", viewName, nowTime+3, 6.0, 7, "row4")
        });
        String testSQL1 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, 21, 1),(%d, 2, 31))", viewName, nowTime, nowTime);
        String testSQL2 = String.format("SELECT ROW_ID FROM %s WHERE (ID2, ID3) IN ((21.0, 1),(2.0, 3))", viewName);
        String testSQL3 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2) IN ((%d, 21.0),(%d, 2.0))", viewName, nowTime+1, nowTime+1);
        String testSQL4 = String.format("SELECT ROW_ID FROM %s WHERE (ID3, ID2, ID1) IN ((3, 21.0, %d),(3, 2.0, %d))", viewName, nowTime+1, nowTime+1);
        String testSQL5 = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, 21.0, 3),(%d, 2.0, 3))", viewName, nowTime+1, nowTime+1);
        String testMaxInList = String.format("SELECT ROW_ID FROM %s WHERE (ID1, ID2, ID3) IN ((%d, 21.0, 3),(%d, 2.0, 3)", viewName, nowTime+1, nowTime+1);

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            tenantConnection.setAutoCommit(true);
            try (Statement ustmt = tenantConnection.createStatement()) {
                for (String upsertSql : UPSERT_SQLS) {
                    ustmt.execute(upsertSql);
                }
            }
        }

        Set<String> expecteds1 = Collections.<String>emptySet();
        Set<String> expecteds2 = Sets.newHashSet(new String[] {"row0", "row1"});
        Set<String> expecteds3 = Sets.newHashSet(new String[] {"row1", "row2"});
        Set<String> expecteds4 = Sets.newHashSet(new String[] {"row1"});
        Set<String> expecteds5 = Sets.newHashSet(new String[] {"row1"});
        Set<String> expecteds6 = Sets.newHashSet(new String[] {"row1"});

        assertExpectedWithWhere(tenantId, testName, testSQL1, expecteds1);
        assertExpectedWithWhere(tenantId, testName, testSQL2, expecteds2);
        assertExpectedWithWhere(tenantId, testName, testSQL3, expecteds3);
        assertExpectedWithWhere(tenantId, testName, testSQL4, expecteds4);
        assertExpectedWithWhere(tenantId, testName, testSQL5, expecteds5);
        PDataType[] testPKTypes = new PDataType[] { PLong.INSTANCE, PDecimal.INSTANCE, PLong.INSTANCE};
        assertExpectedWithMaxInList(tenantId, testName, testPKTypes, testMaxInList, sortOrder, expecteds6);
    }

    private void assertExpectedWithWhere(int tenantId,  String testType, String testSQL, Set<String> expectedSet) throws SQLException {
        String context = "sql: " + testSQL + ", type: " + testType;

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            // perform the query
            ResultSet rs = tenantConnection.createStatement().executeQuery(testSQL);
            for (int i = 0; i < expectedSet.size();i++) {
                assertTrue("did not include result '" + expectedSet.toString() + "' (" + context + ")", rs.next());
                String actual = rs.getString(1);
                assertTrue(context, expectedSet.contains(actual));
            }
            assertFalse(context, rs.next());
        }
    }

    private void assertExpectedWithMaxInList(int tenantId,  String testType, PDataType[] testPKTypes, String testSQL, SortOrder[] sortOrder, Set<String> expected) throws SQLException {
        String context = "sql: " + testSQL + ", type: " + testType + ", sort-order: " + Arrays.stream(sortOrder).map(s -> s.name()).collect(Collectors.joining(","));
        int numInLists = 25;
        boolean expectSkipScan = checkMaxSkipScanCardinality ? Arrays.stream(sortOrder).allMatch(Predicate.isEqual(SortOrder.ASC)) : true;

        StringBuilder query = new StringBuilder(testSQL);
        for (int i = 0; i < numInLists;i++) {
            query.append(",(?,?,?)");
        }
        query.append(")");

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            PhoenixPreparedStatement stmt = tenantConnection.prepareStatement(query.toString()).unwrap(PhoenixPreparedStatement.class);
            // perform the query
            int lastBoundCol = 0;
            setBindVariables(stmt, lastBoundCol, numInLists, testPKTypes);
            QueryPlan plan = stmt.compileQuery(query.toString());
            if (expectSkipScan) {
                assertTrue(plan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY POINT LOOKUP ON"));
            } else {
                assertTrue(plan.getExplainPlan().toString().contains("CLIENT PARALLEL 1-WAY RANGE SCAN OVER"));
            }

            ResultSet rs = stmt.executeQuery(query.toString());
            for (int i = 0; i < expected.size();i++) {
                assertTrue("did not include result '" + expected.toString() + "' (" + context + ")", rs.next());
                String actual = rs.getString(1);
                assertTrue(context, expected.contains(actual));
            }
            assertFalse(context, rs.next());

        }
    }

    @Test
    public void testSkipScanCardinalityOverflow() throws SQLException {
        if (!checkMaxSkipScanCardinality) {
            return;
        }

        final String baseTableName = generateUniqueName();
        final String viewName = String.format("Z_%s", baseTableName);
        int tenantId = 1;

        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            try (Statement cstmt = globalConnection.createStatement()) {
                String createDDL = "CREATE TABLE IF NOT EXISTS " + baseTableName +
                    "(OID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, CREATED_DATE DATE, CREATED_BY CHAR(15), SYSTEM_MODSTAMP DATE " +
                    "CONSTRAINT PK PRIMARY KEY (OID, KP)) MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0";
                cstmt.execute(createDDL);
            }
        }

        String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            try (Statement cstmt = tenantConnection.createStatement()) {
                // DESC order in PK causes key explosion when creating skip scan
                String TENANT_VIEW_TEMPLATE = "CREATE VIEW IF NOT EXISTS %s " +
                    "(ID1 INTEGER not null, ID2 INTEGER not null, ID3 INTEGER not null, ID4 INTEGER not null, ID5 INTEGER not null, COL1 VARCHAR(15) " +
                    "CONSTRAINT pk PRIMARY KEY (ID1 DESC, ID2 DESC, ID3 DESC, ID4 DESC, ID5 DESC)) " +
                    "AS SELECT * FROM %s WHERE KP = 'abc'";
                cstmt.execute(String.format(TENANT_VIEW_TEMPLATE, viewName, baseTableName));
            }

            int totalRows = 100; // 100 ^ 5 ( # of pk cols in view) will cause integer overflow
            String dml = "UPSERT INTO " + viewName +
                "(CREATED_DATE, CREATED_BY, SYSTEM_MODSTAMP, ID1, ID2, ID3, ID4, ID5, COL1)" +
                " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ustmt = tenantConnection.prepareStatement(dml)) {
                long now = EnvironmentEdgeManager.currentTimeMillis();
                for (int i = 0; i < totalRows; i++) {
                    ustmt.setDate(1, new Date(now + i));
                    ustmt.setString(2, "foo");
                    ustmt.setDate(3, new Date(now + i));
                    ustmt.setInt(4, i);
                    ustmt.setInt(5, i);
                    ustmt.setInt(6, i);
                    ustmt.setInt(7, i);
                    ustmt.setInt(8, i);
                    ustmt.setString(9, "COL1_" + i);
                    ustmt.execute();
                }
                tenantConnection.commit();
            }

            StringBuilder selectQuery = new StringBuilder();
            selectQuery.append("SELECT COUNT(*) FROM " + viewName + " WHERE (ID1, ID2, ID3, ID4, ID5) IN (");
            for (int i = 0; i < totalRows; i++) {
                selectQuery.append("(?, ?, ?, ?, ?)");
                selectQuery.append(",");
            }
            // delete the trailing ','
            selectQuery.deleteCharAt(selectQuery.length() - 1);
            selectQuery.append(")");
            try (PreparedStatement selstmt = tenantConnection.prepareStatement(selectQuery.toString())) {
                int paramIndex = 0;
                for (int i = 0; i < totalRows; i++) {
                    selstmt.setInt(++paramIndex, i);
                    selstmt.setInt(++paramIndex, i);
                    selstmt.setInt(++paramIndex, i);
                    selstmt.setInt(++paramIndex, i);
                    selstmt.setInt(++paramIndex, i);
                }
                ResultSet rs = selstmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(totalRows, rs.getInt(1));
            }
        }
    }

    private int setBindVariables(PhoenixPreparedStatement stmt, int startBindIndex, int numBinds, PDataType[] testPKTypes)
            throws SQLException {

        Random rnd = new Random();
        int lastBindCol = 0;
        int numCols = testPKTypes.length;
        for (int i = 0; i<numBinds; i++) {
            for (int b=0;b<testPKTypes.length;b++) {
                int colIndex = startBindIndex + i*numCols+b+1;
                switch (testPKTypes[b].getSqlType()) {
                    case Types.VARCHAR: {
                        // pkTypeStr = "VARCHAR(25)";
                        stmt.setString(colIndex, RandomStringUtils.randomAlphanumeric(25));
                        break;
                    }
                    case Types.CHAR: {
                        //pkTypeStr = "CHAR(15)";
                        stmt.setString(colIndex, RandomStringUtils.randomAlphanumeric(15));
                        break;
                    }
                    case Types.DECIMAL:
                        //pkTypeStr = "DECIMAL(8,2)";
                        stmt.setDouble(colIndex, rnd.nextDouble());
                        break;
                    case Types.INTEGER:
                        //pkTypeStr = "INTEGER";
                        stmt.setInt(colIndex, rnd.nextInt(50000));
                        break;
                    case Types.BIGINT:
                        //pkTypeStr = "BIGINT";
                        stmt.setLong(colIndex, System.currentTimeMillis() + rnd.nextInt(50000));
                        break;
                    case Types.DATE:
                        //pkTypeStr = "DATE";
                        stmt.setDate(colIndex, new Date(System.currentTimeMillis() + rnd.nextInt(50000)));
                        break;
                    case Types.TIMESTAMP:
                        //pkTypeStr = "TIMESTAMP";
                        stmt.setTimestamp(colIndex, new Timestamp(System.currentTimeMillis() + rnd.nextInt(50000)));
                        break;
                    default:
                        // pkTypeStr = "VARCHAR(25)";
                        stmt.setString(colIndex, RandomStringUtils.randomAlphanumeric(25));
                }
                lastBindCol = colIndex;
            }
        }
        return lastBindCol;
    }
}
