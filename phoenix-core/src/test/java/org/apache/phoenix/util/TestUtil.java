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
package org.apache.phoenix.util;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_NAME;
import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.AggregationManager;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementNormalizer;
import org.apache.phoenix.compile.SubqueryRewriter;
import org.apache.phoenix.compile.SubselectRewriter;
import org.apache.phoenix.compile.JoinCompiler.JoinTable;
import org.apache.phoenix.coprocessor.CompactionScanner;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ByteBasedLikeExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.StringBasedLikeExpression;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.expression.function.SumAggregateFunction;
import org.apache.phoenix.filter.MultiCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiEncodedCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiKeyValueComparisonFilter;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.SingleKeyValueComparisonFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PLongColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Objects;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;



public class TestUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtil.class);

    private static final Long ZERO = new Long(0);
    public static final String DEFAULT_SCHEMA_NAME = "S";
    public static final String DEFAULT_DATA_TABLE_NAME = "T";
    public static final String DEFAULT_INDEX_TABLE_NAME = "I";
    public static final String DEFAULT_DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(DEFAULT_SCHEMA_NAME, "T");
    public static final String DEFAULT_INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(DEFAULT_SCHEMA_NAME, "I");

    public static final String TEST_TABLE_SCHEMA = "(" +
        "   varchar_pk VARCHAR NOT NULL, " +
        "   char_pk CHAR(10) NOT NULL, " +
        "   int_pk INTEGER NOT NULL, " +
        "   long_pk BIGINT NOT NULL, " +
        "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
        "   date_pk DATE NOT NULL, " +
        "   a.varchar_col1 VARCHAR, " +
        "   a.char_col1 CHAR(10), " +
        "   a.int_col1 INTEGER, " +
        "   a.long_col1 BIGINT, " +
        "   a.decimal_col1 DECIMAL(31, 10), " +
        "   a.date1 DATE, " +
        "   b.varchar_col2 VARCHAR, " +
        "   b.char_col2 CHAR(10), " +
        "   b.int_col2 INTEGER, " +
        "   b.long_col2 BIGINT, " +
        "   b.decimal_col2 DECIMAL(31, 10), " +
        "   b.date2 DATE " +
        "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk, date_pk)) ";

    private TestUtil() {
    }

    public static final String CF_NAME = "a";
    public static final byte[] CF = Bytes.toBytes(CF_NAME);

    public static final String CF2_NAME = "b";

    public final static String A_VALUE = "a";
    public final static byte[] A = Bytes.toBytes(A_VALUE);
    public final static String B_VALUE = "b";
    public final static byte[] B = Bytes.toBytes(B_VALUE);
    public final static String C_VALUE = "c";
    public final static byte[] C = Bytes.toBytes(C_VALUE);
    public final static String D_VALUE = "d";
    public final static byte[] D = Bytes.toBytes(D_VALUE);
    public final static String E_VALUE = "e";
    public final static byte[] E = Bytes.toBytes(E_VALUE);

    public final static String ROW1 = "00A123122312312";
    public final static String ROW2 = "00A223122312312";
    public final static String ROW3 = "00A323122312312";
    public final static String ROW4 = "00A423122312312";
    public final static String ROW5 = "00B523122312312";
    public final static String ROW6 = "00B623122312312";
    public final static String ROW7 = "00B723122312312";
    public final static String ROW8 = "00B823122312312";
    public final static String ROW9 = "00C923122312312";
    
    public final static String PARENTID1 = "0500x0000000001";
    public final static String PARENTID2 = "0500x0000000002";
    public final static String PARENTID3 = "0500x0000000003";
    public final static String PARENTID4 = "0500x0000000004";
    public final static String PARENTID5 = "0500x0000000005";
    public final static String PARENTID6 = "0500x0000000006";
    public final static String PARENTID7 = "0500x0000000007";
    public final static String PARENTID8 = "0500x0000000008";
    public final static String PARENTID9 = "0500x0000000009";
    
    public final static List<String> PARENTIDS = Lists.newArrayList(PARENTID1, PARENTID2, PARENTID3, PARENTID4, PARENTID5, PARENTID6, PARENTID7, PARENTID8, PARENTID9);
    
    public final static String ENTITYHISTID1 = "017x00000000001";
    public final static String ENTITYHISTID2 = "017x00000000002";
    public final static String ENTITYHISTID3 = "017x00000000003";
    public final static String ENTITYHISTID4 = "017x00000000004";
    public final static String ENTITYHISTID5 = "017x00000000005";
    public final static String ENTITYHISTID6 = "017x00000000006";
    public final static String ENTITYHISTID7 = "017x00000000007";
    public final static String ENTITYHISTID8 = "017x00000000008";
    public final static String ENTITYHISTID9 = "017x00000000009";

    public final static List<String> ENTITYHISTIDS = Lists.newArrayList(ENTITYHISTID1, ENTITYHISTID2, ENTITYHISTID3, ENTITYHISTID4, ENTITYHISTID5, ENTITYHISTID6, ENTITYHISTID7, ENTITYHISTID8, ENTITYHISTID9);
    
    public static final String LOCALHOST = "localhost";
    public static final String PHOENIX_JDBC_URL = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    public static final String PHOENIX_CONNECTIONLESS_JDBC_URL = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;

    public static final String TEST_SCHEMA_FILE_NAME = "config" + File.separator + "test-schema.xml";
    public static final String CED_SCHEMA_FILE_NAME = "config" + File.separator + "schema.xml";
    public static final String ENTITY_HISTORY_TABLE_NAME = "ENTITY_HISTORY";
    public static final String ENTITY_HISTORY_SALTED_TABLE_NAME = "ENTITY_HISTORY_SALTED";
    public static final String ATABLE_NAME = "ATABLE";
    public static final String TABLE_WITH_ARRAY = "TABLE_WITH_ARRAY";
    public static final String SUM_DOUBLE_NAME = "SumDoubleTest";
    public static final String ATABLE_SCHEMA_NAME = "";
    public static final String BTABLE_NAME = "BTABLE";
    public static final String STABLE_NAME = "STABLE";
    public static final String STABLE_PK_NAME = "ID";
    public static final String STABLE_SCHEMA_NAME = "";
    public static final String CUSTOM_ENTITY_DATA_FULL_NAME = "CORE.CUSTOM_ENTITY_DATA";
    public static final String CUSTOM_ENTITY_DATA_NAME = "CUSTOM_ENTITY_DATA";
    public static final String CUSTOM_ENTITY_DATA_SCHEMA_NAME = "CORE";
    public static final String HBASE_NATIVE = "HBASE_NATIVE";
    public static final String HBASE_NATIVE_SCHEMA_NAME = "";
    public static final String HBASE_DYNAMIC_COLUMNS = "HBASE_DYNAMIC_COLUMNS";
    public static final String HBASE_DYNAMIC_COLUMNS_SCHEMA_NAME = "";
    public static final String PRODUCT_METRICS_NAME = "PRODUCT_METRICS";
    public static final String PTSDB_NAME = "PTSDB";
    public static final String PTSDB2_NAME = "PTSDB2";
    public static final String PTSDB3_NAME = "PTSDB3";
    public static final String PTSDB_SCHEMA_NAME = "";
    public static final String FUNKY_NAME = "FUNKY_NAMES";
    public static final String MULTI_CF_NAME = "MULTI_CF";
    public static final String MDTEST_NAME = "MDTEST";
    public static final String MDTEST_SCHEMA_NAME = "";
    public static final String KEYONLY_NAME = "KEYONLY";
    public static final String TABLE_WITH_SALTING = "TABLE_WITH_SALTING";
    public static final String INDEX_DATA_SCHEMA = "INDEX_TEST";
    public static final String INDEX_DATA_TABLE = "INDEX_DATA_TABLE";
    public static final String MUTABLE_INDEX_DATA_TABLE = "MUTABLE_INDEX_DATA_TABLE";
    public static final String TRANSACTIONAL_DATA_TABLE = "TRANSACTIONAL_DATA_TABLE";
    public static final String JOIN_SCHEMA = "Join";
    public static final String JOIN_ORDER_TABLE = "OrderTable";
    public static final String JOIN_CUSTOMER_TABLE = "CustomerTable";
    public static final String JOIN_ITEM_TABLE = "ItemTable";
    public static final String JOIN_SUPPLIER_TABLE = "SupplierTable";
    public static final String JOIN_COITEM_TABLE = "CoitemTable";
    public static final String JOIN_ORDER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_ORDER_TABLE + '"';
    public static final String JOIN_CUSTOMER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_CUSTOMER_TABLE + '"';
    public static final String JOIN_ITEM_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_ITEM_TABLE + '"';
    public static final String JOIN_SUPPLIER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_SUPPLIER_TABLE + '"';
    public static final String JOIN_COITEM_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_COITEM_TABLE + '"';
    public static final String JOIN_ORDER_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_ORDER_TABLE;
    public static final String JOIN_CUSTOMER_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_CUSTOMER_TABLE;
    public static final String JOIN_ITEM_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_ITEM_TABLE;
    public static final String JOIN_SUPPLIER_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_SUPPLIER_TABLE;
    public static final String JOIN_COITEM_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_COITEM_TABLE;
    public static final String BINARY_NAME = "BinaryTable";

    /**
     * Read-only properties used by all tests
     */
    public static final Properties TEST_PROPERTIES = new Properties() {
        @Override
        public String put(Object key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException();
        }
    };

    public static byte[][] getSplits(String tenantId) {
        return new byte[][]{
            HConstants.EMPTY_BYTE_ARRAY,
            Bytes.toBytes(tenantId + "00A"),
            Bytes.toBytes(tenantId + "00B"),
            Bytes.toBytes(tenantId + "00C"),
        };
    }

    public static void assertRoundEquals(BigDecimal bd1, BigDecimal bd2) {
        bd1 = bd1.round(PDataType.DEFAULT_MATH_CONTEXT);
        bd2 = bd2.round(PDataType.DEFAULT_MATH_CONTEXT);
        if (bd1.compareTo(bd2) != 0) {
            fail("expected:<" + bd1 + "> but was:<" + bd2 + ">");
        }
    }

    public static BigDecimal computeAverage(double sum, long count) {
        return BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), PDataType.DEFAULT_MATH_CONTEXT);
    }

    public static BigDecimal computeAverage(long sum, long count) {
        return BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), PDataType.DEFAULT_MATH_CONTEXT);
    }

    public static Expression constantComparison(CompareOperator op, PColumn c, Object o) {
        return new ComparisonExpression(Arrays.<Expression>asList(new KeyValueColumnExpression(c), LiteralExpression.newConstant(o)), op);
    }

    public static Expression kvColumn(PColumn c) {
        return new KeyValueColumnExpression(c);
    }

    public static Expression pkColumn(PColumn c, List<PColumn> columns) {
        return new RowKeyColumnExpression(c, new RowKeyValueAccessor(columns, columns.indexOf(c)));
    }

    public static Expression constantComparison(CompareOperator op, Expression e, Object o) {
        return new ComparisonExpression(Arrays.asList(e, LiteralExpression.newConstant(o)), op);
    }

    private static boolean useByteBasedRegex(StatementContext context) {
        return context
            .getConnection()
            .getQueryServices()
            .getProps()
            .getBoolean(QueryServices.USE_BYTE_BASED_REGEX_ATTRIB,
                QueryServicesOptions.DEFAULT_USE_BYTE_BASED_REGEX);
    }

    public static Expression like(Expression e, Object o, StatementContext context) {
        return useByteBasedRegex(context) ?
            ByteBasedLikeExpression.create(Arrays.asList(e, LiteralExpression.newConstant(o)), LikeType.CASE_SENSITIVE) :
            StringBasedLikeExpression.create(Arrays.asList(e, LiteralExpression.newConstant(o)), LikeType.CASE_SENSITIVE);
    }

    public static Expression ilike(Expression e, Object o, StatementContext context) {
        return useByteBasedRegex(context) ?
            ByteBasedLikeExpression.create(Arrays.asList(e, LiteralExpression.newConstant(o)), LikeType.CASE_INSENSITIVE) :
            StringBasedLikeExpression.create(Arrays.asList(e, LiteralExpression.newConstant(o)), LikeType.CASE_INSENSITIVE);
    }

    public static Expression substr(Expression e, Object offset, Object length) {
        return new SubstrFunction(Arrays.asList(e, LiteralExpression.newConstant(offset), LiteralExpression.newConstant(length)));
    }

    public static Expression substr2(Expression e, Object offset) {

        return new SubstrFunction(Arrays.asList(e, LiteralExpression.newConstant(offset), LiteralExpression.newConstant(null)));
    }

    public static Expression columnComparison(CompareOperator op, Expression c1, Expression c2) {
        return new ComparisonExpression(Arrays.<Expression>asList(c1, c2), op);
    }

    public static SingleKeyValueComparisonFilter singleKVFilter(Expression e) {
        return new SingleCQKeyValueComparisonFilter(e);
    }

    public static RowKeyComparisonFilter rowKeyFilter(Expression e) {
        return new RowKeyComparisonFilter(e, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
    }

    public static MultiKeyValueComparisonFilter multiKVFilter(Expression e) {
        return new MultiCQKeyValueComparisonFilter(e, false, ByteUtil.EMPTY_BYTE_ARRAY);
    }

    public static MultiEncodedCQKeyValueComparisonFilter multiEncodedKVFilter(Expression e, QualifierEncodingScheme encodingScheme) {
        return new MultiEncodedCQKeyValueComparisonFilter(e, encodingScheme, false, null);
    }

    public static Expression and(Expression... expressions) {
        return new AndExpression(Arrays.asList(expressions));
    }

    public static Expression not(Expression expression) {
        return new NotExpression(expression);
    }

    public static Expression or(Expression... expressions) {
        return new OrExpression(Arrays.asList(expressions));
    }

    public static Expression in(Expression... expressions) throws SQLException {
        return InListExpression.create(Arrays.asList(expressions), false, new ImmutableBytesWritable(), true);
    }

    public static Expression in(Expression e, Object... literals) throws SQLException {
        PDataType childType = e.getDataType();
        List<Expression> expressions = new ArrayList<Expression>(literals.length + 1);
        expressions.add(e);
        for (Object o : literals) {
            expressions.add(LiteralExpression.newConstant(o, childType));
        }
        return InListExpression.create(expressions, false, new ImmutableBytesWritable(), true);
    }

    public static void assertDegenerate(StatementContext context) {
        Scan scan = context.getScan();
        assertDegenerate(scan);
    }

    public static void assertDegenerate(Scan scan) {
        assertNull(scan.getFilter());
        assertArrayEquals(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStartRow());
        assertArrayEquals(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStopRow());
        assertEquals(null, scan.getFilter());
    }

    public static void assertNotDegenerate(Scan scan) {
        assertFalse(
            Bytes.compareTo(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStartRow()) == 0 &&
                Bytes.compareTo(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStopRow()) == 0);
    }

    public static void assertEmptyScanKey(Scan scan) {
        assertNull(scan.getFilter());
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, scan.getStartRow());
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, scan.getStopRow());
        assertEquals(null, scan.getFilter());
    }

    /**
     * Does a deep comparison of two Results, down to the byte arrays.
     *
     * @param res1 first result to compare
     * @param res2 second result to compare
     * @throws Exception Every difference is throwing an exception
     */
    public static void compareTuples(Tuple res1, Tuple res2)
        throws Exception {
        if (res2 == null) {
            throw new Exception("There wasn't enough rows, we stopped at "
                + res1);
        }
        if (res1.size() != res2.size()) {
            throw new Exception("This row doesn't have the same number of KVs: "
                + res1.toString() + " compared to " + res2.toString());
        }
        for (int i = 0; i < res1.size(); i++) {
            Cell ourKV = res1.getValue(i);
            Cell replicatedKV = res2.getValue(i);
            if (!ourKV.equals(replicatedKV)) {
                throw new Exception("This result was different: "
                    + res1.toString() + " compared to " + res2.toString());
            }
        }
    }

    public static void clearMetaDataCache(Connection conn) throws Throwable {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        Table htable = pconn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW,
            HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataService, ClearCacheResponse>() {
                @Override
                public ClearCacheResponse call(MetaDataService instance) throws IOException {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<ClearCacheResponse> rpcCallback =
                        new BlockingRpcCallback<ClearCacheResponse>();
                    ClearCacheRequest.Builder builder = ClearCacheRequest.newBuilder();
                    instance.clearCache(controller, builder.build(), rpcCallback);
                    if (controller.getFailedOn() != null) {
                        throw controller.getFailedOn();
                    }
                    return rpcCallback.get();
                }
            });
    }

    public static void closeStatement(Statement stmt) {
        try {
            stmt.close();
        } catch (Throwable ignore) {
        }
    }

    public static void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (Throwable ignore) {
        }
    }

    public static void closeStmtAndConn(Statement stmt, Connection conn) {
        closeStatement(stmt);
        closeConnection(conn);
    }

    public static void bindParams(PhoenixPreparedStatement stmt, List<Object> binds) throws SQLException {
        for (int i = 0; i < binds.size(); i++) {
            stmt.setObject(i + 1, binds.get(i));
        }
    }

    /**
     * @param conn      connection to be used
     * @param sortOrder sort order of column contain input values
     * @param id        id of the row being inserted
     * @param input     input to be inserted
     */
    public static void upsertRow(Connection conn, String tableName, String sortOrder, int id, Object input) throws SQLException {
        String dml = String.format("UPSERT INTO " + tableName + "_%s VALUES(?,?)", sortOrder);
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, id);
        if (input instanceof String)
            stmt.setString(2, (String) input);
        else if (input instanceof Integer)
            stmt.setInt(2, (Integer) input);
        else if (input instanceof Double)
            stmt.setDouble(2, (Double) input);
        else if (input instanceof Float)
            stmt.setFloat(2, (Float) input);
        else if (input instanceof Boolean)
            stmt.setBoolean(2, (Boolean) input);
        else if (input instanceof Long)
            stmt.setLong(2, (Long) input);
        else
            throw new UnsupportedOperationException("" + input.getClass() + " is not supported by upsertRow");
        stmt.execute();
        conn.commit();
    }

    public static void createGroupByTestTable(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute("create table " + tableName +
            "   (id varchar not null primary key,\n" +
            "    uri varchar, appcpu integer)");
    }

    private static void createTable(Connection conn, String inputSqlType, String tableName, String sortOrder) throws SQLException {
        String dmlFormat =
            "CREATE TABLE " + tableName + "_%s (id INTEGER NOT NULL, pk %s NOT NULL, " + "kv %s "
                + "CONSTRAINT PK_CONSTRAINT PRIMARY KEY (id, pk %s))";
        String ddl = String.format(dmlFormat, sortOrder, inputSqlType, inputSqlType, sortOrder);
        conn.createStatement().execute(ddl);
        conn.commit();
    }

    /**
     * Creates a table to be used for testing. It contains one id column, one varchar column to be used as input, and
     * one column which will contain null values
     *
     * @param conn         connection to be used
     * @param inputSqlType sql type of input
     * @param inputList    list of values to be inserted into the pk column
     */
    public static String initTables(Connection conn, String inputSqlType, List<Object> inputList) throws Exception {
        String tableName = generateUniqueName();
        createTable(conn, inputSqlType, tableName, "ASC");
        createTable(conn, inputSqlType, tableName, "DESC");
        for (int i = 0; i < inputList.size(); ++i) {
            upsertRow(conn, tableName, "ASC", i, inputList.get(i));
            upsertRow(conn, tableName, "DESC", i, inputList.get(i));
        }
        return tableName;
    }

    public static List<KeyRange> getAllSplits(Connection conn, String tableName) throws SQLException {
        return getSplits(conn, tableName, null, null, null, null, null);
    }

    public static List<KeyRange> getAllSplits(Connection conn, String tableName, String where, String selectClause) throws SQLException {
        return getSplits(conn, tableName, null, null, null, where, selectClause);
    }

    public static List<KeyRange> getSplits(Connection conn, String tableName, String pkCol, byte[] lowerRange, byte[] upperRange, String whereClauseSuffix, String selectClause) throws SQLException {
        String whereClauseStart =
            (lowerRange == null && upperRange == null ? "" :
                " WHERE " + ((lowerRange != null ? (pkCol + " >= ? " + (upperRange != null ? " AND " : "")) : "")
                    + (upperRange != null ? (pkCol + " < ?") : "")));
        String whereClause = whereClauseSuffix == null ? whereClauseStart : whereClauseStart.length() == 0 ? (" WHERE " + whereClauseSuffix) : (" AND " + whereClauseSuffix);
        String query = "SELECT /*+ NO_INDEX */ " + selectClause + " FROM " + tableName + whereClause;
        PhoenixPreparedStatement pstmt = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
        if (lowerRange != null) {
            pstmt.setBytes(1, lowerRange);
        }
        if (upperRange != null) {
            pstmt.setBytes(lowerRange != null ? 2 : 1, upperRange);
        }
        pstmt.execute();
        List<KeyRange> keyRanges = pstmt.getQueryPlan().getSplits();
        return keyRanges;
    }

    public static Collection<GuidePostsInfo> getGuidePostsList(Connection conn, String tableName) throws SQLException {
        return getGuidePostsList(conn, tableName, null, null, null, null);
    }

    public static Collection<GuidePostsInfo> getGuidePostsList(Connection conn, String tableName, String where)
        throws SQLException {
        return getGuidePostsList(conn, tableName, null, null, null, where);
    }

    public static Collection<GuidePostsInfo> getGuidePostsList(Connection conn, String tableName, String pkCol,
                                                               byte[] lowerRange, byte[] upperRange, String whereClauseSuffix) throws SQLException {
        String whereClauseStart = (lowerRange == null && upperRange == null ? ""
            : " WHERE "
            + ((lowerRange != null ? (pkCol + " >= ? " + (upperRange != null ? " AND " : "")) : "") + (upperRange != null ? (pkCol + " < ?")
            : "")));
        String whereClause = whereClauseSuffix == null ? whereClauseStart
            : whereClauseStart.length() == 0 ? (" WHERE " + whereClauseSuffix) : (" AND " + whereClauseSuffix);
        String query = "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName + whereClause;
        PhoenixPreparedStatement pstmt = conn.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
        if (lowerRange != null) {
            pstmt.setBytes(1, lowerRange);
        }
        if (upperRange != null) {
            pstmt.setBytes(lowerRange != null ? 2 : 1, upperRange);
        }
        pstmt.execute();
        TableRef tableRef = pstmt.getQueryPlan().getTableRef();
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = tableRef.getTable();
        GuidePostsInfo info = pconn.getQueryServices().getTableStats(new GuidePostsKey(table.getName().getBytes(), SchemaUtil.getEmptyColumnFamily(table)));
        return Collections.singletonList(info);
    }

    public static void analyzeTable(Connection conn, String tableName) throws IOException, SQLException {
        analyzeTable(conn, tableName, false);
    }

    public static void analyzeTable(Connection conn, String tableName, boolean transactional) throws IOException, SQLException {
        String query = "UPDATE STATISTICS " + tableName;
        conn.createStatement().execute(query);
        // if the table is transactional burn a txn in order to make sure the next txn read pointer is close to wall clock time
        conn.commit();
    }

    public static void analyzeTableIndex(Connection conn, String tableName) throws IOException, SQLException {
        String query = "UPDATE STATISTICS " + tableName + " INDEX";
        conn.createStatement().execute(query);
    }

    public static void analyzeTableColumns(Connection conn, String tableName) throws IOException, SQLException {
        String query = "UPDATE STATISTICS " + tableName + " COLUMNS";
        conn.createStatement().execute(query);
    }

    public static void analyzeTable(String url, Properties props, String tableName) throws IOException, SQLException {
        Connection conn = DriverManager.getConnection(url, props);
        analyzeTable(conn, tableName);
        conn.close();
    }

    public static void setRowKeyColumns(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setString(1, "varchar" + String.valueOf(i));
        stmt.setString(2, "char" + String.valueOf(i));
        stmt.setInt(3, i);
        stmt.setLong(4, i);
        stmt.setBigDecimal(5, new BigDecimal(i * 0.5d));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * MILLIS_IN_DAY);
        stmt.setDate(6, date);
    }

    public static void validateRowKeyColumns(ResultSet rs, int i) throws SQLException {
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "varchar" + String.valueOf(i));
        assertEquals(rs.getString(2), "char" + String.valueOf(i));
        assertEquals(rs.getInt(3), i);
        assertEquals(rs.getInt(4), i);
        assertEquals(rs.getBigDecimal(5), new BigDecimal(i * 0.5d));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * MILLIS_IN_DAY);
        assertEquals(rs.getDate(6), date);
    }

    public static ClientAggregators getSingleSumAggregator(String url, Properties props) throws SQLException {
        try (PhoenixConnection pconn = DriverManager.getConnection(url, props).unwrap(PhoenixConnection.class)) {
            PhoenixStatement statement = new PhoenixStatement(pconn);
            StatementContext context = new StatementContext(statement, null, new Scan(), new SequenceManager(statement));
            AggregationManager aggregationManager = context.getAggregationManager();
            SumAggregateFunction func = new SumAggregateFunction(Arrays.<Expression>asList(new KeyValueColumnExpression(new PLongColumn() {
                @Override
                public PName getName() {
                    return SINGLE_COLUMN_NAME;
                }

                @Override
                public PName getFamilyName() {
                    return SINGLE_COLUMN_FAMILY_NAME;
                }

                @Override
                public int getPosition() {
                    return 0;
                }

                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }

                @Override
                public Integer getArraySize() {
                    return 0;
                }

                @Override
                public byte[] getViewConstant() {
                    return null;
                }

                @Override
                public boolean isViewReferenced() {
                    return false;
                }

                @Override
                public String getExpressionStr() {
                    return null;
                }

                @Override
                public long getTimestamp() {
                    return HConstants.LATEST_TIMESTAMP;
                }

                @Override
                public boolean isDerived() {
                    return false;
                }

                @Override
                public boolean isExcluded() {
                    return false;
                }

                @Override
                public boolean isRowTimestamp() {
                    return false;
                }

                @Override
                public boolean isDynamic() {
                    return false;
                }

                @Override
                public byte[] getColumnQualifierBytes() {
                    return SINGLE_COLUMN_NAME.getBytes();
                }
            })), null);
            aggregationManager.setAggregators(new ClientAggregators(Collections.<SingleAggregateFunction>singletonList(func), 1));
            ClientAggregators aggregators = aggregationManager.getAggregators();
            return aggregators;
        }
    }

    public static void createMultiCFTestTable(Connection conn, String tableName, String options) throws SQLException {
        String ddl = "create table if not exists " + tableName + "(" +
            "   varchar_pk VARCHAR NOT NULL, " +
            "   char_pk CHAR(5) NOT NULL, " +
            "   int_pk INTEGER NOT NULL, " +
            "   long_pk BIGINT NOT NULL, " +
            "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
            "   a.varchar_col1 VARCHAR, " +
            "   a.char_col1 CHAR(5), " +
            "   a.int_col1 INTEGER, " +
            "   a.long_col1 BIGINT, " +
            "   a.decimal_col1 DECIMAL(31, 10), " +
            "   b.varchar_col2 VARCHAR, " +
            "   b.char_col2 CHAR(5), " +
            "   b.int_col2 INTEGER, " +
            "   b.long_col2 BIGINT, " +
            "   b.decimal_col2 DECIMAL, " +
            "   b.date_col DATE " +
            "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk)) "
            + (options != null ? options : "");
        conn.createStatement().execute(ddl);
    }

    public static void flush(HBaseTestingUtility utility, TableName table) throws IOException {
        Admin admin = utility.getAdmin();
        admin.flush(table);
    }

    public static void minorCompact(HBaseTestingUtility utility, TableName table)
            throws IOException, InterruptedException {
        try {
            CompactionScanner.setForceMinorCompaction(true);
            Admin admin = utility.getAdmin();
            admin.compact(table);
            int waitForCompactionToCompleteCounter = 0;
            while (CompactionScanner.getForceMinorCompaction()) {
                waitForCompactionToCompleteCounter++;
                if (waitForCompactionToCompleteCounter > 50) {
                    Assert.fail();
                }
                Thread.sleep(3000);
            }
        }
        finally {
            CompactionScanner.setForceMinorCompaction(false);
        }
    }

    public static void majorCompact(HBaseTestingUtility utility, TableName table)
        throws IOException, InterruptedException {
        long compactionRequestedSCN = EnvironmentEdgeManager.currentTimeMillis();
        Admin admin = utility.getAdmin();
        admin.majorCompact(table);
        long lastCompactionTimestamp;
        CompactionState state = null;
        CompactionState previousState = null;
        while ((state = admin.getCompactionState(table)).equals(CompactionState.MAJOR)
                || state.equals(CompactionState.MAJOR_AND_MINOR)
                || (lastCompactionTimestamp =
                        admin.getLastMajorCompactionTimestamp(table)) < compactionRequestedSCN) {
            // In HBase 2.5 getLastMajorCompactionTimestamp doesn't seem to get updated when the
            // clock is stopped, so check for the state going to NONE instead
            if (state.equals(CompactionState.NONE) && (previousState != null
                    && previousState.equals(CompactionState.MAJOR_AND_MINOR)
                    || previousState.equals(CompactionState.MAJOR))) {
                break;
            }
            previousState = state;
            Thread.sleep(100);
        }
    }

    /**
     * Runs a major compaction, and then waits until the compaction is complete before returning.
     *
     * @param tableName name of the table to be compacted
     */
    public static void doMajorCompaction(Connection conn, String tableName) throws Exception {

        tableName = SchemaUtil.normalizeIdentifier(tableName);

        // We simply write a marker row, request a major compaction, and then wait until the marker
        // row is gone
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), tableName));
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        MutationState mutationState = pconn.getMutationState();
        if (table.isTransactional()) {
            mutationState.startTransaction(table.getTransactionProvider());
        }
        try (Table htable = mutationState.getHTable(table)) {
            byte[] markerRowKey = Bytes.toBytes("TO_DELETE");

            Put put = new Put(markerRowKey);
            long timestamp = 0L;
            // We do not want to wait an hour because of PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY
            // So set the timestamp of the put and delete as early as possible
            put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                    QueryConstants.EMPTY_COLUMN_VALUE_BYTES, timestamp,
                    QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
            htable.put(put);
            Delete delete = new Delete(markerRowKey);
            delete.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                    QueryConstants.EMPTY_COLUMN_VALUE_BYTES, timestamp);
            htable.delete(delete);
            htable.close();
            if (table.isTransactional()) {
                mutationState.commit();
            }
        
            Admin hbaseAdmin = services.getAdmin();
            hbaseAdmin.flush(TableName.valueOf(tableName));
            hbaseAdmin.majorCompact(TableName.valueOf(tableName));
            hbaseAdmin.close();

            boolean compactionDone = false;
            while (!compactionDone) {
                Thread.sleep(6000L);
                Scan scan = new Scan();
                scan.withStartRow(markerRowKey);
                scan.withStopRow(Bytes.add(markerRowKey, new byte[]{0}));
                scan.setRaw(true);
        
                try (Table htableForRawScan = services.getTable(Bytes.toBytes(tableName))) {
                    ResultScanner scanner = htableForRawScan.getScanner(scan);
                    List<Result> results = Lists.newArrayList(scanner);
                    LOGGER.info("Results: " + results);
                    compactionDone = results.isEmpty();
                    scanner.close();
                }
                LOGGER.info("Compaction done: " + compactionDone);

                // need to run compaction after the next txn snapshot has been written so that compaction can remove deleted rows
                if (!compactionDone && table.isTransactional()) {
                    hbaseAdmin = services.getAdmin();
                    hbaseAdmin.flush(TableName.valueOf(tableName));
                    hbaseAdmin.majorCompact(TableName.valueOf(tableName));
                    hbaseAdmin.close();
                }
            }
        }
    }

    public static void createTransactionalTable(Connection conn, String tableName) throws SQLException {
        createTransactionalTable(conn, tableName, "");
    }

    public static void createTransactionalTable(Connection conn, String tableName, String extraProps) throws SQLException {
        conn.createStatement().execute("create table " + tableName + TestUtil.TEST_TABLE_SCHEMA + "TRANSACTIONAL=true" + (extraProps.length() == 0 ? "" : ("," + extraProps)));
    }

    public static void dumpTable(Connection conn, TableName tableName)
        throws SQLException, IOException {
        ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
        Table table = cqs.getTable(tableName.getName());
        dumpTable(table);
    }

    public static void dumpTable(Table table) throws IOException {
        System.out.println("************ dumping " + table + " **************");
        Scan s = new Scan();
        s.setRaw(true);
        s.readAllVersions();
        int cellCount = 0;
        int rowCount = 0;
        try (ResultScanner scanner = table.getScanner(s)) {
            Result result = null;
            while ((result = scanner.next()) != null) {
                rowCount++;
                CellScanner cellScanner = result.cellScanner();
                Cell current = null;
                while (cellScanner.advance()) {
                    current = cellScanner.current();
                    System.out.println(current + " column= " +
                        Bytes.toStringBinary(CellUtil.cloneQualifier(current)) +
                        " val=" + Bytes.toStringBinary(CellUtil.cloneValue(current)));
                    cellCount++;
                }
            }
        }
        System.out.println("----- Row count: " + rowCount + " Cell count: " + cellCount + " -----");
    }

    public static int getRawRowCount(Table table) throws IOException {
        return getRowCount(table, true);
    }

    public static int getRowCount(Table table, boolean isRaw) throws IOException {
        Scan s = new Scan();
        s.setRaw(isRaw);
        ;
        s.readAllVersions();
        int rows = 0;
        try (ResultScanner scanner = table.getScanner(s)) {
            Result result = null;
            while ((result = scanner.next()) != null) {
                rows++;
                CellScanner cellScanner = result.cellScanner();
                Cell current = null;
                while (cellScanner.advance()) {
                    current = cellScanner.current();
                }
            }
        }
        return rows;
    }

    public static CellCount getCellCount(Table table, boolean isRaw) throws IOException {
        Scan s = new Scan();
        s.setRaw(isRaw);
        s.readAllVersions();

        CellCount cellCount = new CellCount();
        try (ResultScanner scanner = table.getScanner(s)) {
            Result result = null;
            while ((result = scanner.next()) != null) {
                CellScanner cellScanner = result.cellScanner();
                Cell current = null;
                while (cellScanner.advance()) {
                    current = cellScanner.current();
                    cellCount.addCell(Bytes.toString(CellUtil.cloneRow(current)));
                }
            }
        }
        return cellCount;
    }

    static class CellCount {
        private Map<String, Integer> rowCountMap = new HashMap<String, Integer>();

        void addCell(String key) {
            if (rowCountMap.containsKey(key)) {
                rowCountMap.put(key, rowCountMap.get(key) + 1);
            } else {
                rowCountMap.put(key, 1);
            }
        }

        int getCellCount(String key) {
            if (rowCountMap.containsKey(key)) {
                return rowCountMap.get(key);
            } else {
                return 0;
            }
        }
    }


    public static void dumpIndexStatus(Connection conn, String indexName) throws IOException, SQLException {
        try (Table table = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES)) {
            System.out.println("************ dumping index status for " + indexName + " **************");
            Scan s = new Scan();
            s.setRaw(true);
            s.readAllVersions();
            byte[] startRow = SchemaUtil.getTableKeyFromFullName(indexName);
            s.withStartRow(startRow);
            s.withStopRow(ByteUtil.nextKey(ByteUtil.concat(startRow, QueryConstants.SEPARATOR_BYTE_ARRAY)));
            try (ResultScanner scanner = table.getScanner(s)) {
                Result result = null;
                while ((result = scanner.next()) != null) {
                    CellScanner cellScanner = result.cellScanner();
                    Cell current = null;
                    while (cellScanner.advance()) {
                        current = cellScanner.current();
                        if (Bytes.compareTo(current.getQualifierArray(), current.getQualifierOffset(), current.getQualifierLength(), PhoenixDatabaseMetaData.INDEX_STATE_BYTES, 0, PhoenixDatabaseMetaData.INDEX_STATE_BYTES.length) == 0) {
                            System.out.println(current.getTimestamp() + "/INDEX_STATE=" + PIndexState.fromSerializedValue(current.getValueArray()[current.getValueOffset()]));
                        }
                    }
                }
            }
            System.out.println("-----------------------------------------------");
        }
    }

    public static void printResultSet(ResultSet rs) throws SQLException {
        while (rs.next()) {
            printResult(rs, false);
        }
    }

    public static void printResult(ResultSet rs, boolean multiLine) throws SQLException {
        StringBuilder builder = new StringBuilder();
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            Object value = rs.getObject(i + 1);
            String output = value == null ? "null" : value.toString();
            builder.append(output);
            if (i + 1 < columnCount) {
                builder.append(",");
                if (multiLine) {
                    builder.append("\n");
                }
            }
            System.out.println(builder.toString());
        }
    }

    public static void waitForIndexRebuild(Connection conn, String fullIndexName, PIndexState indexState) throws InterruptedException, SQLException {
        waitForIndexState(conn, fullIndexName, indexState, 0L);
    }

    private static class IndexStateCheck {
        public final PIndexState indexState;
        public final Long indexDisableTimestamp;
        public final Boolean success;

        public IndexStateCheck(PIndexState indexState, Long indexDisableTimestamp, Boolean success) {
            this.indexState = indexState;
            this.indexDisableTimestamp = indexDisableTimestamp;
            this.success = success;
        }
    }

    public static void waitForIndexState(Connection conn, String fullIndexName, PIndexState expectedIndexState) throws InterruptedException, SQLException {
        int maxTries = 120, nTries = 0;
        PIndexState actualIndexState = null;
        do {
            String schema = SchemaUtil.getSchemaNameFromFullName(fullIndexName);
            String index = SchemaUtil.getTableNameFromFullName(fullIndexName);
            Thread.sleep(1000); // sleep 1 sec
            String query = "SELECT " + PhoenixDatabaseMetaData.INDEX_STATE + " FROM " +
                PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " WHERE (" + PhoenixDatabaseMetaData.TABLE_SCHEM + "," + PhoenixDatabaseMetaData.TABLE_NAME
                + ") = (" + "'" + schema + "','" + index + "') "
                + "AND " + PhoenixDatabaseMetaData.COLUMN_FAMILY + " IS NULL AND " + PhoenixDatabaseMetaData.COLUMN_NAME + " IS NULL";
            ResultSet rs = conn.createStatement().executeQuery(query);
            if (rs.next()) {
                actualIndexState = PIndexState.fromSerializedValue(rs.getString(1));
                boolean matchesExpected = (actualIndexState == expectedIndexState);
                if (matchesExpected) {
                    return;
                }
            }
        } while (++nTries < maxTries);
        fail("Ran out of time waiting for index state to become " + expectedIndexState + " last seen actual state is " +
            (actualIndexState == null ? "Unknown" : actualIndexState.toString()));
    }

    public static void waitForIndexState(Connection conn, String fullIndexName, PIndexState expectedIndexState, Long expectedIndexDisableTimestamp) throws InterruptedException, SQLException {
        int maxTries = 60, nTries = 0;
        do {
            Thread.sleep(1000); // sleep 1 sec
            IndexStateCheck state = checkIndexStateInternal(conn, fullIndexName, expectedIndexState, expectedIndexDisableTimestamp);
            if (state.success != null) {
                if (Boolean.TRUE.equals(state.success)) {
                    return;
                }
                fail("Index state will not become " + expectedIndexState);
            }
        } while (++nTries < maxTries);
        fail("Ran out of time waiting for index state to become " + expectedIndexState);
    }

    public static boolean checkIndexState(Connection conn, String fullIndexName, PIndexState expectedIndexState, Long expectedIndexDisableTimestamp) throws SQLException {
        return Boolean.TRUE.equals(checkIndexStateInternal(conn, fullIndexName, expectedIndexState, expectedIndexDisableTimestamp).success);
    }

    public static void assertIndexState(Connection conn, String fullIndexName, PIndexState expectedIndexState, Long expectedIndexDisableTimestamp) throws SQLException {
        IndexStateCheck state = checkIndexStateInternal(conn, fullIndexName, expectedIndexState, expectedIndexDisableTimestamp);
        if (!Boolean.TRUE.equals(state.success)) {
            if (expectedIndexState != null) {
                assertEquals(expectedIndexState, state.indexState);
            }
            if (expectedIndexDisableTimestamp != null) {
                assertEquals(expectedIndexDisableTimestamp, state.indexDisableTimestamp);
            }
        }
    }

    public static PIndexState getIndexState(Connection conn, String fullIndexName) throws SQLException {
        IndexStateCheck state = checkIndexStateInternal(conn, fullIndexName, null, null);
        return state.indexState;
    }

    public static long getPendingDisableCount(PhoenixConnection conn, String indexTableName) {
        byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(indexTableName);
        Get get = new Get(indexTableKey);
        get.addColumn(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES);

        try {
            Result pendingDisableCountResult =
                conn.getQueryServices()
                    .getTable(SchemaUtil.getPhysicalTableName(
                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
                        conn.getQueryServices().getProps()).getName())
                    .get(get);
            return Bytes.toLong(pendingDisableCountResult.getValue(TABLE_FAMILY_BYTES,
                PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES));
        } catch (Exception e) {
            LOGGER.error("Exception in getPendingDisableCount: " + e);
            return 0;
        }
    }

    private static IndexStateCheck checkIndexStateInternal(Connection conn, String fullIndexName, PIndexState expectedIndexState, Long expectedIndexDisableTimestamp) throws SQLException {
        String schema = SchemaUtil.getSchemaNameFromFullName(fullIndexName);
        String index = SchemaUtil.getTableNameFromFullName(fullIndexName);
        String query = "SELECT CAST(" + PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP + " AS BIGINT)," + PhoenixDatabaseMetaData.INDEX_STATE + " FROM " +
            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " WHERE (" + PhoenixDatabaseMetaData.TABLE_SCHEM + "," + PhoenixDatabaseMetaData.TABLE_NAME
            + ") = (" + "'" + schema + "','" + index + "') "
            + "AND " + PhoenixDatabaseMetaData.COLUMN_FAMILY + " IS NULL AND " + PhoenixDatabaseMetaData.COLUMN_NAME + " IS NULL";
        ResultSet rs = conn.createStatement().executeQuery(query);
        Long actualIndexDisableTimestamp = null;
        PIndexState actualIndexState = null;
        if (rs.next()) {
            actualIndexDisableTimestamp = rs.getLong(1);
            actualIndexState = PIndexState.fromSerializedValue(rs.getString(2));
            boolean matchesExpected = (expectedIndexDisableTimestamp == null || Objects.equal(actualIndexDisableTimestamp, expectedIndexDisableTimestamp))
                && (expectedIndexState == null || actualIndexState == expectedIndexState);
            if (matchesExpected) {
                return new IndexStateCheck(actualIndexState, actualIndexDisableTimestamp, Boolean.TRUE);
            }
            if (ZERO.equals(actualIndexDisableTimestamp)) {
                return new IndexStateCheck(actualIndexState, actualIndexDisableTimestamp, Boolean.FALSE);
            }
        }
        return new IndexStateCheck(actualIndexState, actualIndexDisableTimestamp, null);
    }

    public static long getRowCount(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ count(*) FROM " + tableName);
        assertTrue(rs.next());
        return rs.getLong(1);
    }

    public static void addCoprocessor(Connection conn, String tableName, Class coprocessorClass) throws Exception {
        int priority = QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY + 100;
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        TableDescriptor descriptor = services.getTableDescriptor(Bytes.toBytes(tableName));
        TableDescriptorBuilder descriptorBuilder = null;
		if (!descriptor.getCoprocessorDescriptors().stream().map(CoprocessorDescriptor::getClassName)
                .collect(Collectors.toList()).contains(coprocessorClass.getName())) {
		    descriptorBuilder=TableDescriptorBuilder.newBuilder(descriptor);
		    descriptorBuilder.setCoprocessor(
                    CoprocessorDescriptorBuilder.newBuilder(coprocessorClass.getName()).setPriority(priority).build());
		}else{
			return;
		}
        final int retries = 10;
        int numTries = 10;
        descriptor = descriptorBuilder.build();
        try (Admin admin = services.getAdmin()) {
            admin.modifyTable(descriptor);
            while (!admin.getDescriptor(TableName.valueOf(tableName)).equals(descriptor)
                    && numTries > 0) {
                numTries--;
                if (numTries == 0) {
                    throw new Exception(
                        "Failed to add " + coprocessorClass.getName() + " after "
                            + retries + " retries.");
                }
                Thread.sleep(1000);
            }
        }
    }

    public static void removeCoprocessor(Connection conn, String tableName, Class coprocessorClass) throws Exception {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        TableDescriptor descriptor = services.getTableDescriptor(Bytes.toBytes(tableName));
        TableDescriptorBuilder descriptorBuilder = null;
        if (descriptor.getCoprocessorDescriptors().stream().map(CoprocessorDescriptor::getClassName)
                .collect(Collectors.toList()).contains(coprocessorClass.getName())) {
            descriptorBuilder=TableDescriptorBuilder.newBuilder(descriptor);
            descriptorBuilder.removeCoprocessor(coprocessorClass.getName());
        }else{
            return;
        }
        final int retries = 10;
        int numTries = retries;
        descriptor = descriptorBuilder.build();
        try (Admin admin = services.getAdmin()) {
            admin.modifyTable(descriptor);
            while (!admin.getDescriptor(TableName.valueOf(tableName)).equals(descriptor)
                    && numTries > 0) {
                numTries--;
                if (numTries == 0) {
                    throw new Exception(
                        "Failed to remove " + coprocessorClass.getName() + " after "
                            + retries + " retries.");
                }
                Thread.sleep(1000);
            }
        }
    }

    public static boolean compare(CompareOperator op, ImmutableBytesWritable lhsOutPtr, ImmutableBytesWritable rhsOutPtr) {
        int compareResult = Bytes.compareTo(lhsOutPtr.get(), lhsOutPtr.getOffset(), lhsOutPtr.getLength(), rhsOutPtr.get(), rhsOutPtr.getOffset(), rhsOutPtr.getLength());
        return ByteUtil.compare(op, compareResult);
    }

    public static QueryPlan getOptimizeQueryPlan(Connection conn, String sql) throws SQLException {
        PhoenixPreparedStatement statement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
        QueryPlan queryPlan = statement.optimizeQuery(sql);
        queryPlan.iterator();
        return queryPlan;
    }

    public static QueryPlan getOptimizeQueryPlanNoIterator(Connection conn, String sql) throws SQLException {
        PhoenixPreparedStatement statement = conn.prepareStatement(sql).unwrap(PhoenixPreparedStatement.class);
        QueryPlan queryPlan = statement.optimizeQuery(sql);
        return queryPlan;
    }

    public static void assertResultSet(ResultSet rs, Object[][] rows) throws Exception {
        for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
            assertTrue("rowIndex:[" + rowIndex + "] rs.next error!", rs.next());
            for (int columnIndex = 1; columnIndex <= rows[rowIndex].length; columnIndex++) {
                Object realValue = rs.getObject(columnIndex);
                Object expectedValue = rows[rowIndex][columnIndex - 1];
                if (realValue == null) {
                    assertNull("rowIndex:[" + rowIndex + "],columnIndex:[" + columnIndex + "]", expectedValue);
                } else {
                    assertEquals("rowIndex:[" + rowIndex + "],columnIndex:[" + columnIndex + "]",
                        expectedValue,
                        realValue
                    );
                }
            }
        }
        assertTrue(!rs.next());
    }

    /**
     * Find a random free port in localhost for binding.
     *
     * @return A port number or -1 for failure.
     */
    public static int getRandomPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    public static boolean hasFilter(Scan scan, Class<? extends Filter> filterClass) {
        Iterator<Filter> filterIter = ScanUtil.getFilterIterator(scan);
        while (filterIter.hasNext()) {
            Filter filter = filterIter.next();
            if (filterClass.isInstance(filter)) {
                return true;
            }
        }
        return false;
    }

    public static JoinTable getJoinTable(String query, PhoenixConnection connection) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement select = SubselectRewriter.flatten(parser.parseQuery(), connection);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, connection);
        select = StatementNormalizer.normalize(select, resolver);
        SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolver, connection);
        if (transformedSelect != select) {
            resolver = FromCompiler.getResolverForQuery(transformedSelect, connection);
            select = StatementNormalizer.normalize(transformedSelect, resolver);
        }
        PhoenixStatement stmt = connection.createStatement().unwrap(PhoenixStatement.class);
        return JoinCompiler.compile(stmt, select, resolver);
    }

    public static void assertSelectStatement(FilterableStatement selectStatement, String sql) {
        assertTrue(selectStatement.toString().trim().equals(sql));
    }

    public static void assertSqlExceptionCode(SQLExceptionCode code, SQLException se) {
        assertEquals(code.getErrorCode(), se.getErrorCode());
        assertTrue("Wrong error message", se.getMessage().contains(code.getMessage()));
        assertEquals(code.getSQLState(), se.getSQLState());
    }

    public static void assertTableHasTtl(Connection conn, TableName tableName, int ttl)
        throws SQLException, IOException {
        ColumnFamilyDescriptor cd = getColumnDescriptor(conn, tableName);
        Assert.assertEquals(ttl, cd.getTimeToLive());
    }

    public static void assertTableHasVersions(Connection conn, TableName tableName, int versions)
        throws SQLException, IOException {
        ColumnFamilyDescriptor cd = getColumnDescriptor(conn, tableName);
        Assert.assertEquals(versions, cd.getMaxVersions());
    }

    public static ColumnFamilyDescriptor getColumnDescriptor(Connection conn, TableName tableName)
        throws SQLException, IOException {
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        TableDescriptor td = admin.getDescriptor(tableName);
        return td.getColumnFamilies()[0];
    }

    public static void assertRawRowCount(Connection conn, TableName table, int expectedRowCount)
        throws SQLException, IOException {
        ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
        int count = TestUtil.getRawRowCount(cqs.getTable(table.getName()));
        assertEquals(expectedRowCount, count);
    }

    public static int getRawCellCount(Connection conn, TableName tableName, byte[] row)
            throws SQLException, IOException {
        ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
        Table table = cqs.getTable(tableName.getName());
        CellCount cellCount = getCellCount(table, true);
        return cellCount.getCellCount(Bytes.toString(row));
    }
    public static void assertRawCellCount(Connection conn, TableName tableName,
                                          byte[] row, int expectedCellCount)
        throws SQLException, IOException {
        int count = getRawCellCount(conn, tableName, row);
        assertEquals(expectedCellCount, count);
    }

    public static void assertRowExistsAtSCN(String url, String sql, long scn, boolean shouldExist)
        throws SQLException {
        boolean rowExists = false;
        Properties props = new Properties();
        ResultSet rs;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        try (Connection conn = DriverManager.getConnection(url, props)) {
            rs = conn.createStatement().executeQuery(sql);
            rowExists = rs.next();
            if (shouldExist) {
                Assert.assertTrue("Row was not found at time " + scn +
                        " when it should have been",
                    rowExists);
            } else {
                Assert.assertFalse("Row was found at time " + scn +
                    " when it should not have been", rowExists);
            }
        }

    }

    public static void assertRowHasExpectedValueAtSCN(String url, String sql,
                                                      long scn, String value) throws SQLException {
        Properties props = new Properties();
        ResultSet rs;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        try (Connection conn = DriverManager.getConnection(url, props)) {
            rs = conn.createStatement().executeQuery(sql);
            Assert.assertTrue("Value " + value + " does not exist at scn " + scn, rs.next());
            Assert.assertEquals(value, rs.getString(1));
        }

    }

    public static String getExplainPlan(Connection conn, String sql) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + sql)){
            return QueryUtil.getExplainPlan(rs);
        }
    }

    public static Path createTempDirectory() throws IOException {
        // We cannot use java.nio.file.Files.createTempDirectory(null),
        // because that caches the value of "java.io.tmpdir" on class load.
        return Files.createTempDirectory(Paths.get(System.getProperty("java.io.tmpdir")), null);
    }

}
