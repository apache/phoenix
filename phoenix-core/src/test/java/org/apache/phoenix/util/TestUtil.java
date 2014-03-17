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

import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheRequest;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.ClearCacheResponse;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MetaDataService;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.filter.MultiCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.MultiKeyValueComparisonFilter;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.SingleKeyValueComparisonFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Lists;



public class TestUtil {
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
    
    public static final long MILLIS_IN_DAY = 1000 * 60 * 60 * 24;
    public static final String LOCALHOST = "localhost";
    public static final String PHOENIX_JDBC_URL = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    public static final String PHOENIX_CONNECTIONLESS_JDBC_URL = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS  + JDBC_PROTOCOL_TERMINATOR  + PHOENIX_TEST_DRIVER_URL_PARAM;

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
    public static final String STABLE_SCHEMA_NAME = "";
    public static final String GROUPBYTEST_NAME = "GROUPBYTEST";
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
    public static final String JOIN_SCHEMA = "Join";
    public static final String JOIN_ORDER_TABLE = "OrderTable";
    public static final String JOIN_CUSTOMER_TABLE = "CustomerTable";
    public static final String JOIN_ITEM_TABLE = "ItemTable";
    public static final String JOIN_SUPPLIER_TABLE = "SupplierTable";
    public static final String JOIN_ORDER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_ORDER_TABLE + '"';
    public static final String JOIN_CUSTOMER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_CUSTOMER_TABLE + '"';
    public static final String JOIN_ITEM_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_ITEM_TABLE + '"';
    public static final String JOIN_SUPPLIER_TABLE_FULL_NAME = '"' + JOIN_SCHEMA + "\".\"" + JOIN_SUPPLIER_TABLE + '"';
    public static final String JOIN_ORDER_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_ORDER_TABLE;
    public static final String JOIN_CUSTOMER_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_CUSTOMER_TABLE;
    public static final String JOIN_ITEM_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_ITEM_TABLE;
    public static final String JOIN_SUPPLIER_TABLE_DISPLAY_NAME = JOIN_SCHEMA + "." + JOIN_SUPPLIER_TABLE;
 
    public static final Properties TEST_PROPERTIES = new Properties();

    public static byte[][] getSplits(String tenantId) {
        return new byte[][] {
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

    public static Expression constantComparison(CompareOp op, PColumn c, Object o) {
        return  new ComparisonExpression(op, Arrays.<Expression>asList(new KeyValueColumnExpression(c), LiteralExpression.newConstant(o)));
    }

    public static Expression kvColumn(PColumn c) {
        return new KeyValueColumnExpression(c);
    }

    public static Expression pkColumn(PColumn c, List<PColumn> columns) {
        return new RowKeyColumnExpression(c, new RowKeyValueAccessor(columns, columns.indexOf(c)));
    }

    public static Expression constantComparison(CompareOp op, Expression e, Object o) {
        return  new ComparisonExpression(op, Arrays.asList(e, LiteralExpression.newConstant(o)));
    }

    public static Expression like(Expression e, Object o) {
        return  new LikeExpression(Arrays.asList(e, LiteralExpression.newConstant(o)));
    }

    public static Expression substr(Expression e, Object offset, Object length) {
        return  new SubstrFunction(Arrays.asList(e, LiteralExpression.newConstant(offset), LiteralExpression.newConstant(length)));
    }

    public static Expression columnComparison(CompareOp op, Expression c1, Expression c2) {
        return  new ComparisonExpression(op, Arrays.<Expression>asList(c1, c2));
    }

    public static SingleKeyValueComparisonFilter singleKVFilter(Expression e) {
        return  new SingleCQKeyValueComparisonFilter(e);
    }

    public static RowKeyComparisonFilter rowKeyFilter(Expression e) {
        return  new RowKeyComparisonFilter(e, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
    }

    public static MultiKeyValueComparisonFilter multiKVFilter(Expression e) {
        return  new MultiCQKeyValueComparisonFilter(e);
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
        return InListExpression.create(Arrays.asList(expressions), false, new ImmutableBytesWritable());
    }

    public static Expression in(Expression e, Object... literals) throws SQLException {
        PDataType childType = e.getDataType();
        List<Expression> expressions = new ArrayList<Expression>(literals.length + 1);
        expressions.add(e);
        for (Object o : literals) {
            expressions.add(LiteralExpression.newConstant(o, childType));
        }
        return InListExpression.create(expressions, false, new ImmutableBytesWritable());
    }

    public static void assertDegenerate(StatementContext context) {
        Scan scan = context.getScan();
        assertDegenerate(scan);
    }

    public static void assertDegenerate(Scan scan) {
        assertNull(scan.getFilter());
        assertArrayEquals(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStartRow());
        assertArrayEquals(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStopRow());
        assertEquals(null,scan.getFilter());
    }

    public static void assertEmptyScanKey(Scan scan) {
        assertNull(scan.getFilter());
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, scan.getStartRow());
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, scan.getStopRow());
        assertEquals(null,scan.getFilter());
    }

    /**
     * Does a deep comparison of two Results, down to the byte arrays.
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
        HTableInterface htable = pconn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        htable.coprocessorService(MetaDataService.class, HConstants.EMPTY_START_ROW,
            HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataService, ClearCacheResponse>() {
                @Override
                public ClearCacheResponse call(MetaDataService instance) throws IOException {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<ClearCacheResponse> rpcCallback =
                            new BlockingRpcCallback<ClearCacheResponse>();
                    ClearCacheRequest.Builder builder = ClearCacheRequest.newBuilder();
                    instance.clearCache(controller, builder.build(), rpcCallback);
                    if(controller.getFailedOn() != null) {
                        throw controller.getFailedOn();
                    }
                    return rpcCallback.get(); 
                }
              });
    }

    public static void closeStatement(Statement stmt) {
        try {
            stmt.close();
        } catch (Throwable ignore) {}
    }
    
    public static void closeConnection(Connection conn) {
        try {
            conn.close();
        } catch (Throwable ignore) {}
    }
    
    public static void closeStmtAndConn(Statement stmt, Connection conn) {
        closeStatement(stmt);
        closeConnection(conn);
    }

    public static void bindParams(PhoenixPreparedStatement stmt, List<Object> binds) throws SQLException {
        for (int i = 0; i < binds.size(); i++) {
            stmt.setObject(i+1, binds.get(i));
        }
    }
}
