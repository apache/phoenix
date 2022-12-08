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
package org.apache.phoenix.compile;

import static org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE;
import static org.apache.phoenix.query.KeyRange.getKeyRange;
import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.BINARY_NAME;
import static org.apache.phoenix.util.TestUtil.BTABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.apache.phoenix.util.TestUtil.assertEmptyScanKey;
import static org.apache.phoenix.util.TestUtil.like;
import static org.apache.phoenix.util.TestUtil.not;
import static org.apache.phoenix.util.TestUtil.rowKeyFilter;
import static org.apache.phoenix.util.TestUtil.substr;
import static org.apache.phoenix.util.TestUtil.substr2;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Optional;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor.KeySlots;
import org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor.SingleKeySlot;
import org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor.SlotsIterator;
import org.apache.phoenix.compile.WhereOptimizer.KeyExpressionVisitor.TrailingRangeIterator;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleCQKeyValueComparisonFilter;
import org.apache.phoenix.filter.SingleKeyValueComparisonFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class WhereOptimizerTest extends BaseConnectionlessQueryTest {

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

    private static final String  TENANT_PREFIX = "Txt00tst1";

    private static StatementContext compileStatement(String query) throws SQLException {
        return compileStatement(query, Collections.emptyList(), null);
    }

    private static StatementContext compileStatement(String query, Integer limit) throws SQLException {
        return compileStatement(query, Collections.emptyList(), limit);
    }

    private static StatementContext compileStatement(String query, List<Object> binds) throws SQLException {
        return compileStatement(query, binds, null);
    }

    private static StatementContext compileStatement(String query, List<Object> binds, Integer limit) throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        assertRoundtrip(query);
        TestUtil.bindParams(pstmt, binds);
        QueryPlan plan = pstmt.compileQuery();
        assertEquals(limit, plan.getLimit());
        return plan.getContext();
    }
  
    @Test
    public void testTrailingRangesIterator() throws Exception {
        KeyRange[] all = new KeyRange[] {EVERYTHING_RANGE,EVERYTHING_RANGE,EVERYTHING_RANGE,EVERYTHING_RANGE,EVERYTHING_RANGE, EVERYTHING_RANGE};
        List<KeyRange[]> singleAll = Collections.singletonList(all);
        KeyRange[] r1 = new KeyRange[] {
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                getKeyRange(Bytes.toBytes("A")),
                EVERYTHING_RANGE, EVERYTHING_RANGE};
        KeyRange[] r2 = new KeyRange[] {
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                getKeyRange(Bytes.toBytes("B")),
                EVERYTHING_RANGE, EVERYTHING_RANGE};
        KeyRange[] r3 = new KeyRange[] {
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                getKeyRange(Bytes.toBytes("C")),
                EVERYTHING_RANGE, EVERYTHING_RANGE};
        KeyRange[] r4 = new KeyRange[] {
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                getKeyRange(Bytes.toBytes("D")),
                EVERYTHING_RANGE, EVERYTHING_RANGE};
        KeyRange[] r5 = new KeyRange[] {
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                EVERYTHING_RANGE,
                getKeyRange(Bytes.toBytes("A"),true,Bytes.toBytes("D"),true),
                EVERYTHING_RANGE, EVERYTHING_RANGE};
        int initPkPos = 1;
        int pkPos = 3;
        List<List<List<KeyRange[]>>> slotsTrailingRangesList = Lists.<List<List<KeyRange[]>>>newArrayList(
                Lists.<List<KeyRange[]>>newArrayList(Lists.<KeyRange[]>newArrayList(r5)),
                Lists.<List<KeyRange[]>>newArrayList(
                        Lists.<KeyRange[]>newArrayList(r1, r2),
                        Lists.<KeyRange[]>newArrayList(r3, r4)
                        ),
                Lists.<List<KeyRange[]>>newArrayList(),
                Lists.<List<KeyRange[]>>newArrayList(singleAll)
                );
        List<KeyRange> results = Lists.<KeyRange>newArrayList();
        List<KeyRange> expectedResults = Lists.newArrayList(getKeyRange(Bytes.toBytes("A")),getKeyRange(Bytes.toBytes("B")),getKeyRange(Bytes.toBytes("C")),getKeyRange(Bytes.toBytes("D")));
        TrailingRangeIterator iterator = new TrailingRangeIterator(initPkPos, pkPos, slotsTrailingRangesList);
        while (iterator.hasNext()) {
            do {
                do {
                    KeyRange range = iterator.getRange();
                    results.add(range);
                } while (iterator.nextTrailingRange());
            } while (iterator.nextRange());
        }
        assertEquals(expectedResults, results);
    }
    
    @Test
    public void testSlotsIterator() throws Exception {
        List<KeySlots> keySlotsList = Lists.newArrayList();
        keySlotsList.add(new SingleKeySlot(null, 0, 
                Lists.<KeyRange>newArrayList(
                        KeyRange.getKeyRange(Bytes.toBytes("A")),
                        KeyRange.getKeyRange(Bytes.toBytes("B"))
                                )));
        keySlotsList.add(new SingleKeySlot(null, 1, 
                Lists.<KeyRange>newArrayList(
                        KeyRange.getKeyRange(Bytes.toBytes("C"))
                                )));
        keySlotsList.add(new SingleKeySlot(null, 0, 
                Lists.<KeyRange>newArrayList(
                        KeyRange.getKeyRange(Bytes.toBytes("D")),
                        KeyRange.getKeyRange(Bytes.toBytes("E"))
                                )));
        keySlotsList.add(new SingleKeySlot(null, 1, 
                Lists.<KeyRange>newArrayList()));
        SlotsIterator iterator = new SlotsIterator(keySlotsList, 0);
        String[][] expectedResults = {
                {"A",null,"D",null},
                {"B",null, "D", null},
                {"A",null,"E",null},
                {"B",null,"E",null},
                };
        int j = 0;
        while (iterator.next()) {
            int i;
            for (i = 0; i < keySlotsList.size(); i++) {
                KeyRange range = iterator.getRange(i);
                String result = range == null ? null : Bytes.toString(range.getLowerRange());
                String expectedResult = expectedResults[j][i];
                assertEquals(expectedResult,result);
            }
            assertEquals(i,expectedResults[j].length);
            j++;
        }
        assertEquals(j, expectedResults.length);
    }
    
    @Test
    public void testMathFunc() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute("create table test (id integer primary key)");
        Scan scan = compileStatement("select ID, exp(ID) from test where exp(ID) < 10").getScan();

        assertNotNull(scan.getFilter());
        assertTrue(scan.getStartRow().length == 0);
        assertTrue(scan.getStopRow().length == 0);
    }
    
    @Test
    public void testSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testGetByteBitExpression() throws SQLException {
        ensureTableCreated(getUrl(), TestUtil.BINARY_NAME, TestUtil.BINARY_NAME);
        int result = 1;
        String query = "select * from " + BINARY_NAME + " where GET_BYTE(a_binary, 0)=" + result;
        Scan scan = compileStatement(query).getScan();

        byte[] tmpBytes, tmpBytes2, tmpBytes3;
        tmpBytes = PInteger.INSTANCE.toBytes(result);
        tmpBytes2 = new byte[16];
        System.arraycopy(tmpBytes, 0, tmpBytes2, 0, tmpBytes.length);
        tmpBytes = ByteUtil.nextKey(tmpBytes);
        tmpBytes3 = new byte[16];
        System.arraycopy(tmpBytes, 0, tmpBytes3, 0, tmpBytes.length);
        assertArrayEquals(tmpBytes2, scan.getStartRow());
        assertArrayEquals(tmpBytes3, scan.getStopRow());

        query = "select * from " + BINARY_NAME + " where GET_BIT(a_binary, 0)=" + result;
        scan = compileStatement(query).getScan();

        tmpBytes = PInteger.INSTANCE.toBytes(result);
        tmpBytes2 = new byte[16];
        System.arraycopy(tmpBytes, 0, tmpBytes2, 0, tmpBytes.length);
        tmpBytes = ByteUtil.nextKey(tmpBytes);
        tmpBytes3 = new byte[16];
        System.arraycopy(tmpBytes, 0, tmpBytes3, 0, tmpBytes.length);
        assertArrayEquals(tmpBytes2, scan.getStartRow());
        assertArrayEquals(tmpBytes3, scan.getStopRow());
    }

    @Test
    public void testDescDecimalRange() throws SQLException {
        String ddl = "create table t (k1 bigint not null, k2 decimal, constraint pk primary key (k1,k2 desc))";
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute(ddl);
        String query = "select * from t where k1 in (1,2) and k2>1.0";
        Scan scan = compileStatement(query).getScan();

        byte[] startRow = ByteUtil.concat(PLong.INSTANCE.toBytes(1), ByteUtil.nextKey(QueryConstants.SEPARATOR_BYTE_ARRAY), QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
        byte[] upperValue = PDecimal.INSTANCE.toBytes(BigDecimal.valueOf(1.0));
        byte[] stopRow = ByteUtil.concat(PLong.INSTANCE.toBytes(2), SortOrder.invert(upperValue,0,upperValue.length), QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
        assertTrue(scan.getFilter() instanceof SkipScanFilter);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testSingleCharPaddedKeyExpression() throws SQLException {
        String tenantId = "1";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] key = StringUtil.padChar(PChar.INSTANCE.toBytes(tenantId), 15);
        assertArrayEquals(key, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(key), scan.getStopRow());
    }

    @Test
    public void testSingleBinaryPaddedKeyExpression() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute("create table bintable (k BINARY(15) PRIMARY KEY)");
        String tenantId = "1";
        String query = "select * from bintable where k='" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] key = ByteUtil.fillKey(PVarchar.INSTANCE.toBytes(tenantId), 15);
        assertArrayEquals(key, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(key), scan.getStopRow());
    }

    @Test
    public void testReverseSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where '" + tenantId + "' = organization_id";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testStartKeyStopKey() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key)");
        conn.close();

        String query = "select * from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        assertArrayEquals(PVarchar.INSTANCE.toBytes("EA"), scan.getStartRow());
        assertArrayEquals(PVarchar.INSTANCE.toBytes("EZ"), scan.getStopRow());
    }

    @Test
    public void testConcatSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id || 'foo' ='" + tenantId + "'||'foo'";
        Scan scan = compileStatement(query).getScan();

        // The || operator cannot currently be used to form the start/stop key
        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testLiteralConcatExpression() throws SQLException {
        String query = "select * from atable where null||'foo'||'bar' = 'foobar'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        compileStatement(query, binds);

        assertNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testSingleKeyNotExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where not organization_id='" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testMultiKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)), 15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testMultiKeyBindExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id=? and substr(entity_id,1,3)=?";
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date startDate = DateUtil.parseDate("2011-12-31 12:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        assertTrue(scan.includeStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testDegenerateRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertDegenerate(scan);
    }

    @Test
    public void testBoundaryGreaterThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date startDate = DateUtil.parseDate("2012-01-01 12:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        assertTrue(scan.includeStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryGreaterThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date startDateHalfRange = DateUtil.parseDate("2011-12-31 12:00:00.000");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(startDateHalfRange));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testGreaterThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date startDate = DateUtil.parseDate("2012-01-01 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);

        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        assertTrue(scan.includeStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLessThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertTrue(scan.includeStartRow());
    }

    @Test
    public void testBoundaryLessThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2011-12-31 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);

        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testLessThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testLessThanOrEqualRound2() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2011-12-31 23:00:00");
        Date endDate = DateUtil.parseDate("2011-12-31 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testBoundaryLessThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date roundDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 12:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,roundDate);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        assertTrue(scan.includeStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testLessThanOrEqualFloor() throws Exception {
        String inst = "a";
        String host = "b";
        Date floorDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and floor(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,floorDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testLessThanOrEqualFloorBoundary() throws Exception {
        String inst = "a";
        String host = "b";
        Date floorDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and floor(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,floorDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testGreaterThanOrEqualFloor() throws Exception {
        String inst = "a";
        String host = "b";
        Date floorDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date startDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and floor(date,'DAY')>=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,floorDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testGreaterThanOrEqualFloorBoundary() throws Exception {
        String inst = "a";
        String host = "b";
        Date floorDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and floor(date,'DAY')>=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,floorDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
            PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
            PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testLessThanOrEqualCeil() throws Exception {
        String inst = "a";
        String host = "b";
        Date ceilDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 00:00:00.001"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and ceil(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,ceilDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testLessThanOrEqualCeilBoundary() throws Exception {
        String inst = "a";
        String host = "b";
        Date ceilDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 00:00:00.001"); //Hbase normalizes scans to left closed
        String query = "select * from ptsdb where inst=? and host=? and ceil(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,ceilDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host)/*,QueryConstants.SEPARATOR_BYTE_ARRAY*/);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testGreaterThanOrEqualCeil() throws Exception {
        String inst = "a";
        String host = "b";
        Date ceilDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00.001");
        String query = "select * from ptsdb where inst=? and host=? and ceil(date,'DAY')>=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,ceilDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testGreaterThanOrEqualCeilBoundary() throws Exception {
        String inst = "a";
        String host = "b";
        Date ceilDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date startDate = DateUtil.parseDate("2011-12-31 00:00:00.001");
        String query = "select * from ptsdb where inst=? and host=? and ceil(date,'DAY')>=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,ceilDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
            PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
            PDate.INSTANCE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
        assertFalse(scan.includeStopRow());
    }

    @Test
    public void testOverlappingKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String entityId = "002333333333333";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "' and entity_id='" + entityId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testSubstrExpressionWithoutLengthVariable() {
        assertEquals("SUBSTR(ENTITY_ID, 1)",((SubstrFunction)substr2(ENTITY_ID,1)).toString());
    }

    @Test
    public void testSubstrExpressionWithLengthVariable() {
        assertEquals("SUBSTR(ENTITY_ID, 1, 10)",((SubstrFunction)substr(ENTITY_ID,1, 10)).toString());
    }

    @Test
    public void testTrailingSubstrExpression() throws SQLException {
        String tenantId = "0xD000000000001";
        String entityId = "002333333333333";
        String query = "select * from atable where substr(organization_id,1,3)='" + tenantId.substring(0, 3) + "' and entity_id='" + entityId + "'";
        Scan scan = compileStatement(query).getScan();
        assertNotNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(StringUtil.padChar(PVarchar.INSTANCE.toBytes(tenantId.substring(0,3)),15),
            PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        // Even though the first slot is a non inclusive range, we need to do a next key
        // on the second slot because of the algorithm we use to seek to and terminate the
        // loop during skip scan. We could end up having a first slot just under the upper
        // limit of slot one and a value equal to the value in slot two and we need this to
        // be less than the upper range that would get formed.
        byte[] stopRow = ByteUtil.concat(StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId.substring(0,3))),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBasicRangeExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id <= '" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        assertTrue(scan.getStartRow().length == 0);
        byte[] stopRow = ByteUtil.concat(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression1() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2= "004";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,3) < '" + keyPrefix2 + "'";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PChar.INSTANCE.toBytes(keyPrefix1),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PChar.INSTANCE.toBytes(keyPrefix2),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2= "004";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,3) <= '" + keyPrefix2 + "'";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PChar.INSTANCE.toBytes(keyPrefix1),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PChar.INSTANCE.toBytes(keyPrefix2)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression3() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2= "004";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) > '" + keyPrefix1 + "' and substr(entity_id,1,3) <= '" + keyPrefix2 + "'";
        Scan scan = compileStatement(query).getScan();
        
        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix1)),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix2)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression4() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) > '" + keyPrefix1 + "' and substr(entity_id,1,3) = '" + entityId + "'";
        Scan scan = compileStatement(query).getScan();
        assertDegenerate(scan);
    }

    @Test
    public void testKeyRangeExpression5() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) <= '" + keyPrefix1 + "' and entity_id = '" + entityId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression6() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) < '" + keyPrefix1 + "' and entity_id = '" + entityId + "'";
        Scan scan = compileStatement(query).getScan();
        assertDegenerate(scan);
    }

    @Test
    public void testKeyRangeExpression7() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) < '" + keyPrefix1 + "' and entity_id < '" + entityId + "'";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = PChar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PChar.INSTANCE.toBytes(keyPrefix1),entityId.length()));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression8() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "001";
        String entityId= "002000000000002";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) > '" + keyPrefix1 + "' and entity_id = '" + entityId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression9() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2 = "0033";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,4) <= '" + keyPrefix2 + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PChar.INSTANCE.toBytes(keyPrefix1),15)); // extra byte is due to implicit internal padding
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PChar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PChar.INSTANCE.toBytes(keyPrefix2)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    /**
     * This is testing the degenerate case where nothing will match because the overlapping keys (keyPrefix and entityId) don't match.
     * @throws SQLException
     */
    @Test
    public void testUnequalOverlappingKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String entityId = "001333333333333";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "' and entity_id='" + entityId + "'";
        Scan scan = compileStatement(query).getScan();
        assertDegenerate(scan);
    }

    @Test
    public void testTopLevelOrKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' or a_integer=2";
        Scan scan = compileStatement(query).getScan();

        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testSiblingOrKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and (a_integer = 2 or a_integer = 3)";
        Scan scan = compileStatement(query).getScan();

        assertNotNull(scan.getFilter());
        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testColumnNotFound() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where bar='" + tenantId + "'";
        try {
            compileStatement(query);
            fail();
        } catch (ColumnNotFoundException e) {
            // expected
        }
    }

    @Test
    public void testNotContiguousPkColumn() throws SQLException {
        String keyPrefix = "002";
        String query = "select * from atable where substr(entity_id,1,3)='" + keyPrefix + "'";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertNotNull(scan.getFilter());
        assertEquals(0, scan.getStartRow().length);
        assertEquals(0, scan.getStopRow().length);
    }

    @Test
    public void testMultipleNonEqualitiesPkColumn() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id >= '" + tenantId + "' AND substr(entity_id,1,3) > '" + keyPrefix + "'";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertNotNull(scan.getFilter());
//        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(
                ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId),
                                PChar.INSTANCE.toBytes(
                                        PChar.INSTANCE.pad(
                                                PChar.INSTANCE.toObject(ByteUtil.nextKey(PChar.INSTANCE.toBytes(keyPrefix))),
                                                15))),
                                                        scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }

    @Test
    public void testRHSLiteral() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 0 >= a_integer limit 1000";
        StatementContext context = compileStatement(query, 1000);
        Scan scan = context.getScan();

        assertNotNull(scan.getFilter());
        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }


    @Test
    public void testKeyTypeMismatch() {
        String query = "select * from atable where organization_id=5";
        try {
            compileStatement(query);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testLikeExtractAllKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + keyPrefix + "%'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeExtractAllKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "中文";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + keyPrefix + "%'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeExtractAllAsEqKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id LIKE ? and entity_id  LIKE '" + keyPrefix + "%'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeNoWildcardExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id LIKE ? and entity_id  LIKE '" + keyPrefix + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        byte[] startRow = ByteUtil.concat(
                PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(startRow);
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeExtractKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String likeArg = keyPrefix + "_";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + likeArg + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertEquals(
                rowKeyFilter(like(
                    ENTITY_ID,
                    likeArg,
                    context)),
                filter);

        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeOptKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String likeArg = keyPrefix + "%003%";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + likeArg + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertEquals(
                rowKeyFilter(like(
                    ENTITY_ID,
                    likeArg,
                    context)),
                filter);

        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeOptKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String likeArg = keyPrefix + "%003%";
        String query = "select * from atable where organization_id = ? and substr(entity_id,1,10)  LIKE '" + likeArg + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertEquals(
                rowKeyFilter(like(
                    substr(ENTITY_ID,1,10),
                    likeArg,
                    context)),
                filter);

        byte[] startRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15));
        byte[] stopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId),StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLikeNoOptKeyExpression3() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String likeArg = keyPrefix + "%003%";
        String query = "select * from atable where organization_id = ? and substr(entity_id,4,10)  LIKE '" + likeArg + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertEquals(
                rowKeyFilter(like(
                    substr(ENTITY_ID,4,10),
                    likeArg,
                    context)),
                filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testLikeNoOptKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String likeArg = "%001%" + keyPrefix + "%";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '" + likeArg + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertEquals(
                rowKeyFilter(like(
                    ENTITY_ID,
                    likeArg,
                    context)),
                filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testLikeNoOptKeyExpression2() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String likeArg = keyPrefix + "%";
        String query = "select * from atable where organization_id = ? and entity_id  NOT LIKE '" + likeArg + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertEquals(
                rowKeyFilter(not(like(
                    ENTITY_ID,
                    likeArg,
                    context))),
                filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(startRow), scan.getStopRow());
    }

    @Test
    public void testLikeDegenerate() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id = ? and entity_id  LIKE '0000000000000012%003%'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateDivision1() throws SQLException {
        String query = "select * from atable where a_integer = 3 / null";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateDivision2() throws SQLException {
        String query = "select * from atable where a_integer / null = 3";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateMult1() throws SQLException {
        String query = "select * from atable where a_integer = 3 * null";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateMult2() throws SQLException {
        String query = "select * from atable where a_integer * null = 3";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateAdd1() throws SQLException {
        String query = "select * from atable where a_integer = 3 + null";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateAdd2() throws SQLException {
        String query = "select * from atable where a_integer + null = 3";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateSub1() throws SQLException {
        String query = "select * from atable where a_integer = 3 - null";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    @Test
    public void testDegenerateSub2() throws SQLException {
        String query = "select * from atable where a_integer - null = 3";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();

        assertDegenerate(scan);
    }

    /*
     * The following 5 tests are testing the comparison in where clauses under the case when the rhs
     * cannot be coerced into the lhs. We need to confirm the decision make by expression compilation
     * returns correct decisions.
     */
    @Test
    public void testValueComparisonInt() throws SQLException {
        ensureTableCreated(getUrl(),"PKIntValueTest", "PKIntValueTest");
        String query;
        // int <-> long
        // Case 1: int = long, comparison always false, key is degenerated.
        query = "SELECT * FROM PKintValueTest where pk = " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 2: int != long, comparison always true, no key set since we need to do a full
        // scan all the time.
        query = "SELECT * FROM PKintValueTest where pk != " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
        // Case 3: int > positive long, comparison always false;
        query = "SELECT * FROM PKintValueTest where pk >= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 4: int <= Integer.MAX_VALUE < positive long, always true;
        query = "SELECT * FROM PKintValueTest where pk <= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
        // Case 5: int >= Integer.MIN_VALUE > negative long, always true;
        query = "SELECT * FROM PKintValueTest where pk >= " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysTrue(query);
        // Case 6: int < negative long, comparison always false;
        query = "SELECT * FROM PKintValueTest where pk <= " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysFalse(query);
    }

    @Test
    public void testValueComparisonUnsignedInt() throws SQLException {
        ensureTableCreated(getUrl(), "PKUnsignedIntValueTest", "PKUnsignedIntValueTest");
        String query;
        // unsigned_int <-> negative int/long
        // Case 1: unsigned_int = negative int, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk = -1";
        assertQueryConditionAlwaysFalse(query);
        // Case 2: unsigned_int != negative int, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk != -1";
        assertQueryConditionAlwaysTrue(query);
        // Case 3: unsigned_int > negative int, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk > " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysTrue(query);
        // Case 4: unsigned_int < negative int, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk < " + + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysFalse(query);
        // unsigned_int <-> big positive long
        // Case 1: unsigned_int = big positive long, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk = " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 2: unsigned_int != big positive long, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk != " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
        // Case 3: unsigned_int > big positive long, always false;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk >= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysFalse(query);
        // Case 4: unsigned_int < big positive long, always true;
        query = "SELECT * FROM PKUnsignedIntValueTest where pk <= " + Long.MAX_VALUE;
        assertQueryConditionAlwaysTrue(query);
    }

    @Test
    public void testValueComparisonUnsignedLong() throws SQLException {
        ensureTableCreated(getUrl(), "PKUnsignedLongValueTest", "PKUnsignedLongValueTest");
        String query;
        // unsigned_long <-> positive int/long
        // Case 1: unsigned_long = negative int/long, always false;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk = -1";
        assertQueryConditionAlwaysFalse(query);
        // Case 2: unsigned_long = negative int/long, always true;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk != " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysTrue(query);
        // Case 3: unsigned_long > negative int/long, always true;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk > -1";
        assertQueryConditionAlwaysTrue(query);
        // Case 4: unsigned_long < negative int/long, always false;
        query = "SELECT * FROM PKUnsignedLongValueTest where pk < " + (Long.MIN_VALUE + 1);
        assertQueryConditionAlwaysFalse(query);
    }

    private void assertQueryConditionAlwaysTrue(String query) throws SQLException {
        Scan scan = compileStatement(query).getScan();
        assertEmptyScanKey(scan);
    }

    private void assertQueryConditionAlwaysFalse(String query) throws SQLException {
        Scan scan = compileStatement(query).getScan();
        assertDegenerate(scan);
    }
    
    @Test
    public void testOrSameColExpression() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000003";
        String query = "select * from atable where organization_id = ? or organization_id  = ?";
        List<Object> binds = Arrays.<Object>asList(tenantId1,tenantId2);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        
        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertNotNull(scanRanges);
        List<List<KeyRange>> ranges = scanRanges.getRanges();
        assertEquals(1,ranges.size());
        List<List<KeyRange>> expectedRanges = Collections.singletonList(Arrays.asList(
                PChar.INSTANCE.getKeyRange(PChar.INSTANCE.toBytes(tenantId1), true, PChar.INSTANCE.toBytes(tenantId1), true),
                PChar.INSTANCE.getKeyRange(PChar.INSTANCE.toBytes(tenantId2), true, PChar.INSTANCE.toBytes(tenantId2), true)));
        assertEquals(expectedRanges, ranges);
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId2)), scan.getStopRow());
    }
    
    @Test
    public void testAndOrExpression() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000003";
        String entityId1 = "002333333333331";
        String entityId2 = "002333333333333";
        String query = "select * from atable where (organization_id = ? and entity_id  = ?) or (organization_id = ? and entity_id  = ?)";
        List<Object> binds = Arrays.<Object>asList(tenantId1,entityId1,tenantId2,entityId2);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();

        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        
        ScanRanges scanRanges = context.getScanRanges();
        assertEquals(ScanRanges.EVERYTHING,scanRanges);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testOrDiffColExpression() throws SQLException {
        String tenantId1 = "000000000000001";
        String entityId1 = "002333333333331";
        String query = "select * from atable where organization_id = ? or entity_id  = ?";
        List<Object> binds = Arrays.<Object>asList(tenantId1,entityId1);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();

        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertEquals(ScanRanges.EVERYTHING,scanRanges);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testOrSameColRangeExpression() throws SQLException {
        String query = "select * from atable where substr(organization_id,1,3) = ? or organization_id LIKE 'foo%'";
        List<Object> binds = Arrays.<Object>asList("00D");
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();

        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertNotNull(scanRanges);
        List<List<KeyRange>> ranges = scanRanges.getRanges();
        assertEquals(1,ranges.size());
        List<List<KeyRange>> expectedRanges = Collections.singletonList(Arrays.asList(
                PChar.INSTANCE.getKeyRange(
                        StringUtil.padChar(PChar.INSTANCE.toBytes("00D"),15), true,
                        StringUtil.padChar(ByteUtil.nextKey(PChar.INSTANCE.toBytes("00D")),15), false),
                PChar.INSTANCE.getKeyRange(
                        StringUtil.padChar(PChar.INSTANCE.toBytes("foo"),15), true,
                        StringUtil.padChar(ByteUtil.nextKey(PChar.INSTANCE.toBytes("foo")),15), false)));
        assertEquals(expectedRanges, ranges);
    }

    @Test
    public void testOrPKRanges() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        ensureTableCreated(getUrl(), TestUtil.BTABLE_NAME);
        Statement stmt = conn.createStatement();
        // BTABLE has 5 PK columns
        String query = "select * from " + BTABLE_NAME +
                       " where (a_string > '1' and a_string < '5') or (a_string > '6' and a_string < '9')";
        StatementContext context = compileStatement(query);
        Filter filter = context.getScan().getFilter();

        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertNotNull(scanRanges);
        List<List<KeyRange>> ranges = scanRanges.getRanges();
        assertEquals(1, ranges.size());
        List<List<KeyRange>> expectedRanges = Collections.singletonList(Arrays.asList(
                KeyRange.getKeyRange(Bytes.toBytes("1"), false, Bytes.toBytes("5"), false),
                KeyRange.getKeyRange(Bytes.toBytes("6"), false, Bytes.toBytes("9"), false)));
        assertEquals(expectedRanges, ranges);

        stmt.close();
        conn.close();
    }
    
    @Test
    public void testOrPKRangesNotOptimized() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        ensureTableCreated(getUrl(), TestUtil.BTABLE_NAME);
        Statement stmt = conn.createStatement();
        // BTABLE has 5 PK columns
        String[] queries = {
                "select * from " + BTABLE_NAME + " where (a_string > '1' and a_string < '5') or (a_string > '6' and a_string < '9' and a_id = 'foo')",
                "select * from " + BTABLE_NAME + " where (a_id > 'aaa' and a_id < 'ccc') or (a_id > 'jjj' and a_id < 'mmm')",
                };
        for (String query : queries) {
            StatementContext context = compileStatement(query);
            Iterator<Filter> it = ScanUtil.getFilterIterator(context.getScan());
            while (it.hasNext()) {
                assertFalse(it.next() instanceof SkipScanFilter);
            }
            TestUtil.assertNotDegenerate(context.getScan());
        }

        stmt.close();
        conn.close();
    }
    
    @Test
    public void testForceSkipScanOnSaltedTable() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS user_messages (\n" + 
                "        SENDER_ID UNSIGNED_LONG NOT NULL,\n" + 
                "        RECIPIENT_ID UNSIGNED_LONG NOT NULL,\n" + 
                "        SENDER_IP VARCHAR,\n" + 
                "        IS_READ VARCHAR,\n" + 
                "        IS_DELETED VARCHAR,\n" + 
                "        M_TEXT VARCHAR,\n" + 
                "        M_TIMESTAMP timestamp  NOT NULL,\n" + 
                "        ROW_ID UNSIGNED_LONG NOT NULL\n" + 
                "        constraint rowkey primary key (SENDER_ID,RECIPIENT_ID,M_TIMESTAMP DESC,ROW_ID))\n" + 
                "SALT_BUCKETS=12\n");
        String query = "select /*+ SKIP_SCAN */ count(*) from user_messages where is_read='N' and recipient_id=5399179882";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        
        assertNotNull(filter);
        assertTrue(filter instanceof FilterList);
        FilterList filterList = (FilterList)filter;
        assertEquals(FilterList.Operator.MUST_PASS_ALL, filterList.getOperator());
        assertEquals(2, filterList.getFilters().size());
        assertTrue(filterList.getFilters().get(0) instanceof SkipScanFilter);
        assertTrue(filterList.getFilters().get(1) instanceof SingleKeyValueComparisonFilter);
        
        ScanRanges scanRanges = context.getScanRanges();
        assertNotNull(scanRanges);
        assertEquals(3,scanRanges.getRanges().size());
        assertEquals(1,scanRanges.getRanges().get(1).size());
        assertEquals(KeyRange.EVERYTHING_RANGE,scanRanges.getRanges().get(1).get(0));
        assertEquals(1,scanRanges.getRanges().get(2).size());
        assertTrue(scanRanges.getRanges().get(2).get(0).isSingleKey());
        assertEquals(Long.valueOf(5399179882L), PUnsignedLong.INSTANCE.toObject(scanRanges.getRanges().get(2).get(0).getLowerRange()));
    }
    
    @Test
    public void testForceRangeScanKeepsFilters() throws SQLException {
        ensureTableCreated(getUrl(), TestUtil.ENTITY_HISTORY_TABLE_NAME, TestUtil.ENTITY_HISTORY_TABLE_NAME);
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select /*+ RANGE_SCAN */ ORGANIZATION_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID from " + TestUtil.ENTITY_HISTORY_TABLE_NAME + 
                " where ORGANIZATION_ID=? and SUBSTR(PARENT_ID, 1, 3) = ? and  CREATED_DATE >= ? and CREATED_DATE < ? order by ORGANIZATION_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID limit 6";
        Date startTime = new Date(System.currentTimeMillis());
        Date stopTime = new Date(startTime.getTime() + MILLIS_IN_DAY);
        List<Object> binds = Arrays.<Object>asList(tenantId, keyPrefix, startTime, stopTime);
        StatementContext context = compileStatement(query, binds, 6);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);

        byte[] expectedStartRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId), StringUtil.padChar(PVarchar.INSTANCE.toBytes(keyPrefix),15), PDate.INSTANCE.toBytes(startTime));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        byte[] expectedStopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId), StringUtil.padChar(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(keyPrefix)),15));
        assertArrayEquals(expectedStopRow, scan.getStopRow());
    }

    @Test
    public void testBasicRVCExpression() throws SQLException {
        String tenantId = "000000000000001";
        String entityId = "002333333333331";
        String query = "select * from atable where (organization_id,entity_id) >= (?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, entityId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        assertNull(scan.getFilter());
        byte[] expectedStartRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId), PChar.INSTANCE.toBytes(entityId));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }

    
    @Test
    public void testRVCExpressionThroughOr() throws SQLException {
        String tenantId =  "000000000000001";
        String entityId =  "002333333333331";
        String entityId1 = "002333333333330";
        String entityId2 = "002333333333332";
        String query = "select * from atable where (organization_id,entity_id) >= (?,?) and organization_id = ? and  (entity_id = ? or entity_id = ?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, entityId, tenantId, entityId1, entityId2);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        byte[] expectedStartRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId1));
        byte[] expectedStopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId2), QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(expectedStopRow, scan.getStopRow());
        Filter filter = scan.getFilter();
        assertTrue(filter instanceof SkipScanFilter);
        SkipScanFilter skipScanFilter = (SkipScanFilter)filter;
        List<List<KeyRange>> skipScanRanges = Arrays.asList(
                Arrays.asList(KeyRange.getKeyRange(ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId1))),
                              KeyRange.getKeyRange(ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(entityId2)))));
        assertEquals(skipScanRanges, skipScanFilter.getSlots());
    }
    
    @Test
    public void testNotRepresentableBySkipScan() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName + "(a INTEGER NOT NULL, b INTEGER NOT NULL, CONSTRAINT pk PRIMARY KEY (a,b))");
        String query = "SELECT * FROM " + tableName + 
                " WHERE (a,b) >= (1,5) and (a,b) < (3,8) and (a = 1 or a = 3) and ((b >= 6 and b < 9) or (b > 3 and b <= 5))";
        StatementContext context = compileStatement(query);
        Scan scan = context.getScan();
        byte[] expectedStartRow = ByteUtil.concat(PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(4));
        byte[] expectedStopRow = ByteUtil.concat(PInteger.INSTANCE.toBytes(3), PInteger.INSTANCE.toBytes(9));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(expectedStopRow, scan.getStopRow());
        Filter filter = scan.getFilter();
        assertTrue(filter instanceof FilterList);
        FilterList filterList = (FilterList)filter;
        // We can form a skip scan, but it's not exact, so we need the boolean expression filter
        // as well.
        assertTrue(filterList.getFilters().get(0) instanceof SkipScanFilter);
        assertTrue(filterList.getFilters().get(1) instanceof BooleanExpressionFilter);
        SkipScanFilter skipScanFilter = (SkipScanFilter)filterList.getFilters().get(0);
        List<List<KeyRange>> skipScanRanges = Arrays.asList(
                Arrays.asList(KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(1)),
                              KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(3))),
                Arrays.asList(KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(5), true),
                              KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(6), true, PInteger.INSTANCE.toBytes(9), false)));
        assertEquals(skipScanRanges, skipScanFilter.getSlots());
    }
    
    /**
     * With only a subset of row key cols present (which includes the leading key), 
     * Phoenix should have optimized the start row for the scan to include the 
     * row keys cols that occur contiguously in the RVC.
     * 
     * Table entity_history has the row key defined as (organization_id, parent_id, created_date, entity_history_id). 
     * This test uses (organization_id, parent_id, entity_id) in RVC. So the start row should be comprised of
     * organization_id and parent_id.
     * @throws SQLException
     */
    @Test
    public void testRVCExpressionWithSubsetOfPKCols() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000002";
        String entityHistId = "000000000000003";
        
        String query = "select * from entity_history where (organization_id, parent_id, entity_history_id) >= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId, entityHistId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        byte[] expectedStartRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(parentId));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    /**
     * With the leading row key col missing Phoenix won't be able to optimize
     * and provide the start row for the scan.
     * 
     * Table entity_history has the row key defined as (organization_id, parent_id, created_date, entity_history_id). 
     * This test uses (parent_id, entity_id) in RVC. Start row should be empty.
     * @throws SQLException
     */
    
    @Test
    public void testRVCExpressionWithoutLeadingColOfRowKey() throws SQLException {
        
        String parentId = "000000000000002";
        String entityHistId = "000000000000003";
        
        String query = "select * from entity_history where (parent_id, entity_history_id) >= (?,?)";
        List<Object> binds = Arrays.<Object>asList(parentId, entityHistId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testRVCExpressionWithNonFirstLeadingColOfRowKey() throws SQLException {
        String old_value = "value";
        String orgId = getOrganizationId();
        
        String query = "select * from entity_history where (old_value, organization_id) >= (?,?)";
        List<Object> binds = Arrays.<Object>asList(old_value, orgId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof SingleKeyValueComparisonFilter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testMultiRVCExpressionsCombinedWithAnd() throws SQLException {
        String lowerTenantId = "000000000000001";
        String lowerParentId = "000000000000002";
        Date lowerCreatedDate = new Date(System.currentTimeMillis());
        String upperTenantId = "000000000000008";
        String upperParentId = "000000000000009";
        
        String query = "select * from entity_history where (organization_id, parent_id, created_date) >= (?, ?, ?) AND (organization_id, parent_id) <= (?, ?)";
        List<Object> binds = Arrays.<Object>asList(lowerTenantId, lowerParentId, lowerCreatedDate, upperTenantId, upperParentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] expectedStartRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(lowerTenantId), PVarchar.INSTANCE.toBytes(lowerParentId), PDate.INSTANCE.toBytes(lowerCreatedDate));
        byte[] expectedStopRow = ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(upperTenantId), PVarchar.INSTANCE.toBytes(upperParentId)));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(expectedStopRow, scan.getStopRow());
    }
    
    @Test
    public void testMultiRVCExpressionsCombinedUsingLiteralExpressions() throws SQLException {
        String lowerTenantId = "000000000000001";
        String lowerParentId = "000000000000002";
        Date lowerCreatedDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (organization_id, parent_id, created_date) >= (?, ?, ?) AND (organization_id, parent_id) <= ('7', '7')";
        List<Object> binds = Arrays.<Object>asList(lowerTenantId, lowerParentId, lowerCreatedDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] expectedStartRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(lowerTenantId), PVarchar.INSTANCE.toBytes(lowerParentId), PDate.INSTANCE.toBytes(lowerCreatedDate));
        byte[] expectedStopRow = ByteUtil.nextKey(ByteUtil.concat(StringUtil.padChar(PVarchar.INSTANCE.toBytes("7"),15), StringUtil.padChar(
            PVarchar.INSTANCE.toBytes("7"), 15)));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(expectedStopRow, scan.getStopRow());
    }
    
    @Test
    public void testUseOfFunctionOnLHSInRVC() throws SQLException {
        String tenantId = "000000000000001";
        String subStringTenantId = tenantId.substring(0, 3);
        String parentId = "000000000000002";
        Date createdDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (substr(organization_id, 1, 3), parent_id, created_date) >= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(subStringTenantId, parentId, createdDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        byte[] expectedStartRow = PVarchar.INSTANCE.toBytes(subStringTenantId);
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testUseOfFunctionOnLHSInMiddleOfRVC() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000002";
        String subStringParentId = parentId.substring(0, 3);
        Date createdDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (organization_id, substr(parent_id, 1, 3), created_date) >= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, subStringParentId, createdDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        byte[] expectedStartRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(subStringParentId));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testUseOfFunctionOnLHSInMiddleOfRVCForLTE() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000002";
        String subStringParentId = parentId.substring(0, 3);
        Date createdDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (organization_id, substr(parent_id, 1, 3), created_date) <= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, subStringParentId, createdDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);
        byte[] expectedStopRow = ByteUtil.concat(
            PVarchar.INSTANCE.toBytes(tenantId), ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(subStringParentId)));
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStartRow());
        assertArrayEquals(expectedStopRow, scan.getStopRow());
    }
    
    @Test
    public void testNullAtEndOfRVC() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000002";
        Date createdDate = null;
        
        String query = "select * from entity_history where (organization_id, parent_id, created_date) >= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId, createdDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] expectedStartRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), PVarchar.INSTANCE.toBytes(parentId));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testNullInMiddleOfRVC() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = null;
        Date createdDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (organization_id, parent_id, created_date) >= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId, createdDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] expectedStartRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId), new byte[15], ByteUtil.previousKey(
            PDate.INSTANCE.toBytes(createdDate)));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testNullAtStartOfRVC() throws SQLException {
        String tenantId = null;
        String parentId = "000000000000002";
        Date createdDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (organization_id, parent_id, created_date) >= (?,?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId, createdDate);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] expectedStartRow = ByteUtil.concat(new byte[15], ByteUtil.previousKey(PChar.INSTANCE.toBytes(parentId)), PDate.INSTANCE.toBytes(createdDate));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testRVCInCombinationWithOtherNonRVC() throws SQLException {
        String firstOrgId = "000000000000001";
        String secondOrgId = "000000000000008";
        
        String parentId = "000000000000002";
        Date createdDate = new Date(System.currentTimeMillis());
        
        String query = "select * from entity_history where (organization_id, parent_id, created_date) >= (?,?,?) AND organization_id <= ?";
        List<Object> binds = Arrays.<Object>asList(firstOrgId, parentId, createdDate, secondOrgId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.concat(PVarchar.INSTANCE.toBytes(firstOrgId), PVarchar.INSTANCE.toBytes(parentId), PDate.INSTANCE.toBytes(createdDate)), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(secondOrgId)), scan.getStopRow());
    }
    
    @Test
    public void testGreaterThanEqualTo_NonRVCOnLHSAndRVCOnRHS_WithNonNullBindParams() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000008";
        
        String query = "select * from entity_history where organization_id >= (?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testGreaterThan_NonRVCOnLHSAndRVCOnRHS_WithNonNullBindParams() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000008";
        
        String query = "select * from entity_history where organization_id > (?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testGreaterThan() throws SQLException {
        String tenantId = "000000000000001";
        
        String query = "select * from entity_history where organization_id >?";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testLessThanEqualTo_NonRVCOnLHSAndRVCOnRHS_WithNonNullBindParams() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000008";
        
        String query = "select * from entity_history where organization_id <= (?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }
    
    @Test
    public void testLessThan_NonRVCOnLHSAndRVCOnRHS_WithNonNullBindParams() throws SQLException {
        String tenantId = "000000000000001";
        String parentId = "000000000000008";
        
        String query = "select * from entity_history where organization_id < (?,?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, parentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }
    
    @Test
    public void testQueryMoreRVC() throws SQLException {
        String ddl = "CREATE TABLE rvcTestIdx "
                + " (\n" + 
                "    pk1 VARCHAR NOT NULL,\n" + 
                "    v1 VARCHAR,\n" + 
                "    pk2 DECIMAL NOT NULL,\n" + 
                "    CONSTRAINT PK PRIMARY KEY \n" + 
                "    (\n" + 
                "        pk1,\n" + 
                "        v1,\n" + 
                "        pk2\n" + 
                "    )\n" + 
                ") MULTI_TENANT=true,IMMUTABLE_ROWS=true";
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute(ddl);
        String query = "SELECT pk1, pk2, v1 FROM rvcTestIdx WHERE pk1 = 'a' AND\n" + 
                "(pk1, pk2) > ('a', 1)\n" + 
                "ORDER BY PK1, PK2\n" + 
                "LIMIT 2";
        StatementContext context = compileStatement(query, 2);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        byte[] startRow = Bytes.toBytes("a");
        byte[] stopRow = ByteUtil.concat(startRow, ByteUtil.nextKey(QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(stopRow, scan.getStopRow());
    }
    
    @Test
    public void testCombiningRVCUsingOr() throws SQLException {
        String firstTenantId = "000000000000001";
        String secondTenantId = "000000000000005";
        String firstParentId = "000000000000011";
        String secondParentId = "000000000000015";
        
        String query = "select * from entity_history where (organization_id, parent_id) >= (?,?) OR (organization_id, parent_id) <= (?, ?)";
        List<Object> binds = Arrays.<Object>asList(firstTenantId, firstParentId, secondTenantId, secondParentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testCombiningRVCUsingOr2() throws SQLException {
        String firstTenantId = "000000000000001";
        String secondTenantId = "000000000000005";
        String firstParentId = "000000000000011";
        String secondParentId = "000000000000015";
        
        String query = "select * from entity_history where (organization_id, parent_id) >= (?,?) OR (organization_id, parent_id) >= (?, ?)";
        List<Object> binds = Arrays.<Object>asList(firstTenantId, firstParentId, secondTenantId, secondParentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.concat(PVarchar.INSTANCE.toBytes(firstTenantId), PVarchar.INSTANCE.toBytes(firstParentId)), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testCombiningRVCWithNonRVCUsingOr() throws SQLException {
        String firstTenantId = "000000000000001";
        String secondTenantId = "000000000000005";
        String firstParentId = "000000000000011";
        
        String query = "select * from entity_history where (organization_id, parent_id) >= (?,?) OR organization_id  >= ?";
        List<Object> binds = Arrays.<Object>asList(firstTenantId, firstParentId, secondTenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.concat(PVarchar.INSTANCE.toBytes(firstTenantId), PVarchar.INSTANCE.toBytes(firstParentId)), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testCombiningRVCWithNonRVCUsingOr2() throws SQLException {
        String firstTenantId = "000000000000001";
        String secondTenantId = "000000000000005";
        String firstParentId = "000000000000011";
        
        String query = "select * from entity_history where (organization_id, parent_id) >= (?,?) OR organization_id  <= ?";
        List<Object> binds = Arrays.<Object>asList(firstTenantId, firstParentId, secondTenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }
    
    @Test
    public void testCombiningRVCWithNonRVCUsingOr3() throws SQLException {
        String firstTenantId = "000000000000005";
        String secondTenantId = "000000000000001";
        String firstParentId = "000000000000011";
        String query = "select * from entity_history where (organization_id, parent_id) >= (?,?) OR organization_id  <= ?";
        List<Object> binds = Arrays.<Object>asList(firstTenantId, firstParentId, secondTenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertTrue(filter instanceof SkipScanFilter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
        SkipScanFilter skipScanFilter = (SkipScanFilter)filter;
        List<List<KeyRange>> keyRanges = skipScanFilter.getSlots();
        assertEquals(1, keyRanges.size());
        assertEquals(2, keyRanges.get(0).size());
        KeyRange range1 = keyRanges.get(0).get(0);
        KeyRange range2 = keyRanges.get(0).get(1);
        assertEquals(KeyRange.getKeyRange(KeyRange.UNBOUND, false, Bytes.toBytes(secondTenantId), true), range1);
        assertEquals(KeyRange.getKeyRange(ByteUtil.concat(Bytes.toBytes(firstTenantId), Bytes.toBytes(firstParentId)), true, KeyRange.UNBOUND, true), range2);
    }

    @Test
    public void testUsingRVCNonFullyQualifiedInClause() throws Exception {
        String firstOrgId = "000000000000001";
        String secondOrgId = "000000000000009";
        String firstParentId = "000000000000011";
        String secondParentId = "000000000000021";
        String query = "select * from entity_history where (organization_id, parent_id) IN ((?, ?), (?, ?))";
        List<Object> binds = Arrays.<Object>asList(firstOrgId, firstParentId, secondOrgId, secondParentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertTrue(filter instanceof SkipScanFilter);
        assertArrayEquals(ByteUtil.concat(PVarchar.INSTANCE.toBytes(firstOrgId), PVarchar.INSTANCE.toBytes(firstParentId)), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(ByteUtil.concat(PVarchar.INSTANCE.toBytes(secondOrgId), PVarchar.INSTANCE.toBytes(secondParentId))), scan.getStopRow());
    }
    
    @Test
    public void testUsingRVCFullyQualifiedInClause() throws Exception {
        String firstOrgId = "000000000000001";
        String secondOrgId = "000000000000009";
        String firstParentId = "000000000000011";
        String secondParentId = "000000000000021";
        String query = "select * from atable where (organization_id, entity_id) IN ((?, ?), (?, ?))";
        List<Object> binds = Arrays.<Object>asList(firstOrgId, firstParentId, secondOrgId, secondParentId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertTrue(filter instanceof SkipScanFilter);
        List<List<KeyRange>> skipScanRanges = Collections.singletonList(Arrays.asList(
                KeyRange.getKeyRange(ByteUtil.concat(PChar.INSTANCE.toBytes(firstOrgId), PChar.INSTANCE.toBytes(firstParentId))),
                KeyRange.getKeyRange(ByteUtil.concat(PChar.INSTANCE.toBytes(secondOrgId), PChar.INSTANCE.toBytes(secondParentId)))));
        assertEquals(skipScanRanges, context.getScanRanges().getRanges());
        assertArrayEquals(ByteUtil.concat(PChar.INSTANCE.toBytes(firstOrgId), PChar.INSTANCE.toBytes(firstParentId)), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(PChar.INSTANCE.toBytes(secondOrgId), PChar.INSTANCE.toBytes(secondParentId), QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
    }
    
    @Test
    public void testFullyQualifiedRVCWithTenantSpecificViewAndConnection() throws Exception {
    	String baseTableDDL = "CREATE TABLE BASE_MULTI_TENANT_TABLE(\n " + 
                "  tenant_id VARCHAR(5) NOT NULL,\n" + 
                "  userid INTEGER NOT NULL,\n" + 
                "  username VARCHAR NOT NULL,\n" +
                "  col VARCHAR\n " + 
                "  CONSTRAINT pk PRIMARY KEY (tenant_id, userid, username)) MULTI_TENANT=true";
    	Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(baseTableDDL);
        conn.close();
        
        String tenantId = "tenantId";
        String tenantViewDDL = "CREATE VIEW TENANT_VIEW AS SELECT * FROM BASE_MULTI_TENANT_TABLE";
        Properties tenantProps = new Properties();
        tenantProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        conn = DriverManager.getConnection(getUrl(), tenantProps);
        conn.createStatement().execute(tenantViewDDL);
        
        String query = "SELECT * FROM TENANT_VIEW WHERE (userid, username) IN ((?, ?), (?, ?))";
        List<Object> binds = Arrays.<Object>asList(1, "uname1", 2, "uname2");
        
        StatementContext context = compileStatementTenantSpecific(tenantId, query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertEquals(SkipScanFilter.class, filter.getClass());
    }
    
    @Test
    public void testFullyQualifiedRVCWithNonTenantSpecificView() throws Exception {
    	String baseTableDDL = "CREATE TABLE BASE_TABLE(\n " + 
                "  tenant_id VARCHAR(5) NOT NULL,\n" + 
                "  userid INTEGER NOT NULL,\n" + 
                "  username VARCHAR NOT NULL,\n" +
                "  col VARCHAR\n " + 
                "  CONSTRAINT pk PRIMARY KEY (tenant_id, userid, username))";
    	Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(baseTableDDL);
        conn.close();
        
        String viewDDL = "CREATE VIEW VIEWXYZ AS SELECT * FROM BASE_TABLE";
        conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(viewDDL);
        
        String query = "SELECT * FROM VIEWXYZ WHERE (tenant_id, userid, username) IN ((?, ?, ?), (?, ?, ?))";
        List<Object> binds = Arrays.<Object>asList("tenantId", 1, "uname1", "tenantId", 2, "uname2");
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertEquals(SkipScanFilter.class, filter.getClass());
    }
    
    @Test
    public void testRVCWithCompareOpsForRowKeyColumnValuesSmallerThanSchema() throws SQLException {
        String orgId = "0000005";
        String entityId = "011";
        String orgId2 = "000005";
        String entityId2 = "11";
        
        // CASE 1: >=
        String query = "select * from atable where (organization_id, entity_id) >= (?,?)";
        List<Object> binds = Arrays.<Object>asList(orgId, entityId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.concat(StringUtil.padChar(PChar.INSTANCE.toBytes(orgId), 15), StringUtil.padChar(
            PChar.INSTANCE.toBytes(entityId), 15)), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
        
        // CASE 2: >
        query = "select * from atable where (organization_id, entity_id) > (?,?)";
        binds = Arrays.<Object>asList(orgId, entityId);
        context = compileStatement(query, binds);
        scan = context.getScan();
        filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(ByteUtil.nextKey(ByteUtil.concat(StringUtil.padChar(PChar.INSTANCE.toBytes(orgId), 15), StringUtil.padChar(
            PChar.INSTANCE.toBytes(entityId), 15))), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
        
        // CASE 3: <=
        query = "select * from atable where (organization_id, entity_id) <= (?,?)";
        binds = Arrays.<Object>asList(orgId, entityId);
        context = compileStatement(query, binds);
        scan = context.getScan();
        filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(ByteUtil.concat(StringUtil.padChar(PChar.INSTANCE.toBytes(orgId), 15), StringUtil.padChar(
            PChar.INSTANCE.toBytes(entityId), 15))), scan.getStopRow());
        
        // CASE 4: <
        query = "select * from atable where (organization_id, entity_id) < (?,?)";
        binds = Arrays.<Object>asList(orgId, entityId);
        context = compileStatement(query, binds);
        scan = context.getScan();
        filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(StringUtil.padChar(PChar.INSTANCE.toBytes(orgId), 15), StringUtil.padChar(
            PChar.INSTANCE.toBytes(entityId), 15)), scan.getStopRow());
        
        // CASE 5: =
        // For RVC, this will only occur if there's more than one key in the IN
        query = "select * from atable where (organization_id, entity_id) IN ((?,?),(?,?))";
        binds = Arrays.<Object>asList(orgId, entityId, orgId2, entityId2);
        context = compileStatement(query, binds);
        scan = context.getScan();
        filter = scan.getFilter();
        assertTrue(filter instanceof SkipScanFilter);
        ScanRanges scanRanges = context.getScanRanges();
        assertEquals(2,scanRanges.getPointLookupCount());
        Iterator<KeyRange> iterator = scanRanges.getPointLookupKeyIterator();
        KeyRange k1 = iterator.next();
        assertTrue(k1.isSingleKey());
        assertArrayEquals(ByteUtil.concat(StringUtil.padChar(PChar.INSTANCE.toBytes(orgId), 15), StringUtil.padChar(
            PChar.INSTANCE.toBytes(entityId), 15)), k1.getLowerRange());
        KeyRange k2 = iterator.next();
        assertTrue(k2.isSingleKey());
        assertArrayEquals(ByteUtil.concat(StringUtil.padChar(PChar.INSTANCE.toBytes(orgId2), 15), StringUtil.padChar(
            PChar.INSTANCE.toBytes(entityId2), 15)), k2.getLowerRange());
    }
    
    
    @Test
    public void testRVCInView() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE TEST_TABLE.TEST1 (\n" + 
                "PK1 CHAR(3) NOT NULL, \n" + 
                "PK2 CHAR(3) NOT NULL,\n" + 
                "DATA1 CHAR(10)\n" + 
                "CONSTRAINT PK PRIMARY KEY (PK1, PK2))");
        conn.createStatement().execute("CREATE VIEW TEST_TABLE.FOO AS SELECT * FROM TEST_TABLE.TEST1 WHERE PK1 = 'FOO'");
        String query = "SELECT * FROM TEST_TABLE.FOO WHERE PK2 < '004' AND (PK1,PK2) > ('FOO','002') LIMIT 2";
        Scan scan = compileStatement(query, Collections.emptyList(), 2).getScan();
        byte[] startRow = ByteUtil.nextKey(ByteUtil.concat(PChar.INSTANCE.toBytes("FOO"),
                PVarchar.INSTANCE.toBytes("002")));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PChar.INSTANCE.toBytes("FOO"),
                PChar.INSTANCE.toBytes("004"));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    private static StatementContext compileStatementTenantSpecific(String tenantId, String query, List<Object> binds) throws Exception {
    	PhoenixConnection pconn = getTenantSpecificConnection("tenantId").unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        TestUtil.bindParams(pstmt, binds);
        QueryPlan plan = pstmt.compileQuery();
        return  plan.getContext();
    }
    
    private static Connection getTenantSpecificConnection(String tenantId) throws Exception {
    	Properties tenantProps = new Properties();
        tenantProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        Connection conn = DriverManager.getConnection(getUrl(), tenantProps);
        return conn;
    }
    
    @Test
    public void testTrailingIsNull() throws Exception {
        String baseTableDDL = "CREATE TABLE t(\n " + 
                "  a VARCHAR,\n" + 
                "  b VARCHAR,\n" + 
                "  CONSTRAINT pk PRIMARY KEY (a, b))";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(baseTableDDL);
        conn.close();
        
        String query = "SELECT * FROM t WHERE a = 'a' and b is null";
        StatementContext context = compileStatement(query, Collections.<Object>emptyList());
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        assertArrayEquals(Bytes.toBytes("a"), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("a"), QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
    }
    
    
    @Test
    public void testTrailingIsNullWithOr() throws Exception {
        String baseTableDDL = "CREATE TABLE t(\n " + 
                "  a VARCHAR,\n" + 
                "  b VARCHAR,\n" + 
                "  CONSTRAINT pk PRIMARY KEY (a, b))";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(baseTableDDL);
        conn.close();
        
        String query = "SELECT * FROM t WHERE a = 'a' and (b is null or b = 'b')";
        StatementContext context = compileStatement(query, Collections.<Object>emptyList());
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertTrue(filter instanceof SkipScanFilter);
        SkipScanFilter skipScan = (SkipScanFilter)filter;
        List<List<KeyRange>>slots = skipScan.getSlots();
        assertEquals(2,slots.size());
        assertEquals(1,slots.get(0).size());
        assertEquals(2,slots.get(1).size());
        assertEquals(KeyRange.getKeyRange(Bytes.toBytes("a")), slots.get(0).get(0));
        assertTrue(KeyRange.IS_NULL_RANGE == slots.get(1).get(0));
        assertEquals(KeyRange.getKeyRange(Bytes.toBytes("b")), slots.get(1).get(1));
        assertArrayEquals(Bytes.toBytes("a"), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(Bytes.toBytes("a"), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes("b"), QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
    }
    
    @Test
    public void testAndWithRVC() throws Exception {
        String ddl;
        String query;
        StatementContext context;        
        Connection conn = DriverManager.getConnection(getUrl());

        ddl = "create table t (a integer not null, b integer not null, c integer constraint pk primary key (a,b))";
        conn.createStatement().execute(ddl);
        
        query = "select c from t where a in (1,2) and b = 3 and (a,b) in ( (1,2) , (1,3))";
        context = compileStatement(query, Collections.<Object>emptyList());
        assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(3)), context.getScan().getStartRow());
        assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), ByteUtil.nextKey(PInteger.INSTANCE.toBytes(3))), context.getScan().getStopRow());

        query = "select c from t where (a,b) in ( (1,2) , (1,3) ) and b = 4";
        context = compileStatement(query, Collections.<Object>emptyList());
        assertDegenerate(context.getScan());
        
        query = "select c from t where a = 1 and b = 3 and (a,b) in ( (1,2) , (1,3))";
        context = compileStatement(query, Collections.<Object>emptyList());
        assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(3)), context.getScan().getStartRow());
        assertArrayEquals(ByteUtil.concat(PInteger.INSTANCE.toBytes(1), ByteUtil.nextKey(PInteger.INSTANCE.toBytes(3))), context.getScan().getStopRow());

        // Test with RVC occurring later in the PK
        ddl = "create table t1 (d varchar, e char(3) not null, a integer not null, b integer not null, c integer constraint pk primary key (d, e, a,b))";
        conn.createStatement().execute(ddl);
        
        query = "select c from t1 where d = 'a' and e = 'foo' and a in (1,2) and b = 3 and (a,b) in ( (1,2) , (1,3))";
        context = compileStatement(query, Collections.<Object>emptyList());
        Scan scan = context.getScan();
        assertArrayEquals(ByteUtil.concat(PVarchar.INSTANCE.toBytes("a"), QueryConstants.SEPARATOR_BYTE_ARRAY, PChar.INSTANCE.toBytes("foo"), PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(3)), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(PVarchar.INSTANCE.toBytes("a"), QueryConstants.SEPARATOR_BYTE_ARRAY, PChar.INSTANCE.toBytes("foo"), PInteger.INSTANCE.toBytes(1), ByteUtil.nextKey(PInteger.INSTANCE.toBytes(3))), scan.getStopRow());
        
        conn.close();
    }

    @Test
    public void testNoAggregatorForOrderBy() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute("create table test (pk1 integer not null, pk2 integer not null, constraint pk primary key (pk1,pk2))");
        StatementContext context = compileStatement("select count(distinct pk1) from test order by count(distinct pk2)");
        assertEquals(1, context.getAggregationManager().getAggregators().getAggregatorCount());
        context = compileStatement("select sum(pk1) from test order by count(distinct pk2)");
        assertEquals(1, context.getAggregationManager().getAggregators().getAggregatorCount());
        context = compileStatement("select min(pk1) from test order by count(distinct pk2)");
        assertEquals(1, context.getAggregationManager().getAggregators().getAggregatorCount());
        context = compileStatement("select max(pk1) from test order by count(distinct pk2)");
        assertEquals(1, context.getAggregationManager().getAggregators().getAggregatorCount());
        // here the ORDER BY is not optimized away
        context = compileStatement("select avg(pk1) from test order by count(distinct pk2)");
        assertEquals(2, context.getAggregationManager().getAggregators().getAggregatorCount());
    }
    
    @Test
    public void testPartialRVCWithLeadingPKEq() throws SQLException {
        String tenantId = "o1";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE COMMUNITIES.TEST (\n" + 
                "    ORGANIZATION_ID CHAR(2) NOT NULL,\n" + 
                "    SCORE DOUBLE NOT NULL,\n" + 
                "    ENTITY_ID CHAR(2) NOT NULL\n" + 
                "    CONSTRAINT PAGE_SNAPSHOT_PK PRIMARY KEY (\n" + 
                "        ORGANIZATION_ID,\n" + 
                "        SCORE,\n" + 
                "        ENTITY_ID\n" + 
                "    )\n" + 
                ") VERSIONS=1, MULTI_TENANT=TRUE");
        String query = "SELECT entity_id, score\n" + 
                "FROM communities.test\n" + 
                "WHERE organization_id = '" + tenantId + "'\n" + 
                "AND (score, entity_id) > (2.0, '04')\n" + 
                "ORDER BY score, entity_id";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.nextKey(ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId), PDouble.INSTANCE.toBytes(2.0), PChar.INSTANCE.toBytes("04")));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testPartialRVCWithLeadingPKEqDesc() throws SQLException {
        String tenantId = "o1";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE COMMUNITIES.TEST (\n" + 
                "    ORGANIZATION_ID CHAR(2) NOT NULL,\n" + 
                "    SCORE DOUBLE NOT NULL,\n" + 
                "    ENTITY_ID CHAR(2) NOT NULL\n" + 
                "    CONSTRAINT PAGE_SNAPSHOT_PK PRIMARY KEY (\n" + 
                "        ORGANIZATION_ID,\n" + 
                "        SCORE DESC,\n" + 
                "        ENTITY_ID DESC\n" + 
                "    )\n" + 
                ") VERSIONS=1, MULTI_TENANT=TRUE");
        String query = "SELECT entity_id, score\n" + 
                "FROM communities.test\n" + 
                "WHERE organization_id = '" + tenantId + "'\n" + 
                "AND (score, entity_id) < (2.0, '04')\n" + 
                "ORDER BY score DESC, entity_id DESC";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.nextKey(ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId), PDouble.INSTANCE.toBytes(2.0, SortOrder.DESC), PChar.INSTANCE.toBytes("04", SortOrder.DESC)));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testFullRVCWithLeadingPKEqDesc() throws SQLException {
        String tenantId = "o1";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE COMMUNITIES.TEST (\n" + 
                "    ORGANIZATION_ID CHAR(2) NOT NULL,\n" + 
                "    SCORE DOUBLE NOT NULL,\n" + 
                "    ENTITY_ID CHAR(2) NOT NULL\n" + 
                "    CONSTRAINT PAGE_SNAPSHOT_PK PRIMARY KEY (\n" + 
                "        ORGANIZATION_ID,\n" + 
                "        SCORE DESC,\n" + 
                "        ENTITY_ID DESC\n" + 
                "    )\n" + 
                ") VERSIONS=1, MULTI_TENANT=TRUE");
        String query = "SELECT entity_id, score\n" + 
                "FROM communities.test\n" + 
                "WHERE organization_id = '" + tenantId + "'\n" + 
                "AND (organization_id, score, entity_id) < ('" + tenantId + "',2.0, '04')\n" + 
                "ORDER BY score DESC, entity_id DESC";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        // TODO: end to end test that confirms this start row is accurate
        byte[] startRow = ByteUtil.concat(PChar.INSTANCE.toBytes(tenantId), PDouble.INSTANCE.toBytes(2.0, SortOrder.DESC), ByteUtil.nextKey(PChar.INSTANCE.toBytes("04", SortOrder.DESC)));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }
    
    @Test
    public void testTrimTrailing() throws Exception {
        try (Connection conn= DriverManager.getConnection(getUrl())) {
            String sql="CREATE TABLE T("+
                    "A CHAR(1) NOT NULL,"+
                    "B CHAR(1) NOT NULL,"+
                    "C CHAR(1) NOT NULL,"+
                    "D CHAR(1) NOT NULL,"+
                    "DATA INTEGER, "+
                    "CONSTRAINT TEST_PK PRIMARY KEY (A,B,C,D))";
            conn.createStatement().execute(sql);

            // Will cause trailing part of RVC to (A,B,C) to be trimmed allowing us to perform a skip scan
            sql="select * from T where (A,B,C) >= ('A','A','A') and (A,B,C) < ('D','D','D') and (B,C) > ('E','E')";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            Scan scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            List<List<KeyRange>> rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(PChar.INSTANCE.toBytes("A"), true, PChar.INSTANCE.toBytes("D"), false)
                                ),
                        Arrays.asList(
                                KeyRange.getKeyRange(PChar.INSTANCE.toBytes("EE"), false, KeyRange.UNBOUND, false)
                                )
                            ),
                     rowKeyRanges
                    );
            assertArrayEquals(scan.getStartRow(), PChar.INSTANCE.toBytes("AEF"));
            assertArrayEquals(scan.getStopRow(), PChar.INSTANCE.toBytes("D"));
            sql="select * from T where (A,B,C) > ('A','A','A') and (A,B,C) <= ('D','D','D') and (B,C) >= ('E','E')";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(PChar.INSTANCE.toBytes("A"), false, PChar.INSTANCE.toBytes("D"), true)
                                ),
                        Arrays.asList(
                                KeyRange.getKeyRange(PChar.INSTANCE.toBytes("EE"), true, KeyRange.UNBOUND, false)
                                )
                            ),
                     rowKeyRanges
                    );
            assertArrayEquals(PChar.INSTANCE.toBytes("BEE"), scan.getStartRow());
            assertArrayEquals(PChar.INSTANCE.toBytes("E"), scan.getStopRow());
        }
    }

    @Test
    public void testMultiSlotTrailingIntersect() throws Exception {
        try (Connection conn= DriverManager.getConnection(getUrl())) {
            String sql="CREATE TABLE T("+
                    "A CHAR(1) NOT NULL,"+
                    "B CHAR(1) NOT NULL,"+
                    "C CHAR(1) NOT NULL,"+
                    "D CHAR(1) NOT NULL,"+
                    "DATA INTEGER, "+
                    "CONSTRAINT TEST_PK PRIMARY KEY (A,B,C,D))";
            conn.createStatement().execute(sql);

            sql = "select * from t where (a,b) in (('A','B'),('B','A'),('B','B'),('A','A')) and (a,b,c) in ( ('A','B','C') , ('A','C','D'), ('B','B','E'))";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            Scan scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            List<List<KeyRange>> rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.POINT.apply(PChar.INSTANCE.toBytes("ABC")),
                                KeyRange.POINT.apply(PChar.INSTANCE.toBytes("BBE"))
                                )
                            ),
                     rowKeyRanges
                    );
            assertArrayEquals(scan.getStartRow(), PChar.INSTANCE.toBytes("ABC"));
            assertArrayEquals(scan.getStopRow(), PChar.INSTANCE.toBytes("BBF"));
        }
    }

    @Test
    public void testEqualityAndGreaterThanRVC() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE T (\n" + 
                    "    A CHAR(1) NOT NULL,\n" + 
                    "    B CHAR(1) NOT NULL,\n" + 
                    "    C CHAR(1) NOT NULL,\n" + 
                    "    D CHAR(1) NOT NULL,\n" + 
                    "    CONSTRAINT PK PRIMARY KEY (\n" + 
                    "        A,\n" + 
                    "        B,\n" + 
                    "        C,\n" + 
                    "        D\n" + 
                    "    )\n" + 
                    ")");
            String query = "SELECT * FROM T WHERE A = 'C' and (A,B,C) > ('C','B','X') and C='C'";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, query);
            Scan scan = queryPlan.getContext().getScan();
            assertArrayEquals(ByteUtil.concat(PChar.INSTANCE.toBytes("C"), PChar.INSTANCE.toBytes("C"), PChar.INSTANCE.toBytes("C")), scan.getStartRow());
            assertArrayEquals(PChar.INSTANCE.toBytes("D"), scan.getStopRow());
        }
    }
    
    @Test
    public void testOrExpressionNonLeadingPKPushToScanBug4602() throws Exception {
        Connection conn = null;
        try {
            conn= DriverManager.getConnection(getUrl());
            String testTableName="OR_NO_LEADING_PK4602";
            String sql="CREATE TABLE "+ testTableName +"("+
                    "PK1 INTEGER NOT NULL,"+
                    "PK2 INTEGER NOT NULL,"+
                    "PK3 INTEGER NOT NULL,"+
                    "DATA INTEGER, "+
                    "CONSTRAINT TEST_PK PRIMARY KEY (PK1,PK2,PK3))";
            conn.createStatement().execute(sql);
            
            //case 1: pk1 is equal,pk2 is multiRange
            sql="select * from "+testTableName+" t where (t.pk1 = 2) and ((t.pk2 >= 4 and t.pk2 <6) or (t.pk2 >= 8 and t.pk2 <9))";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            Scan scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            List<List<KeyRange>> rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(KeyRange.POINT.apply(PInteger.INSTANCE.toBytes(2))),
                        Arrays.asList(
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(6), false),
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(8), true, PInteger.INSTANCE.toBytes(9), false)
                                )
                            ),
                     rowKeyRanges
                    );

            assertArrayEquals(scan.getStartRow(), ByteUtil.concat(PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(4)));
            assertArrayEquals(scan.getStopRow(), ByteUtil.concat(PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(9)));

            //case 2: pk1 is range,pk2 is multiRange
            sql="select * from "+testTableName+" t where (t.pk1 >=2 and t.pk1<5) and ((t.pk2 >= 4 and t.pk2 <6) or (t.pk2 >= 8 and t.pk2 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(2), true, PInteger.INSTANCE.toBytes(5), false)),
                        Arrays.asList(
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(6), false),
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(8), true, PInteger.INSTANCE.toBytes(9), false)
                                )
                            ),
                     rowKeyRanges
                    );
            assertArrayEquals(scan.getStartRow(), ByteUtil.concat(PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(4)));
            assertArrayEquals(scan.getStopRow(), PInteger.INSTANCE.toBytes(5));

             //case 3 : pk1 has multiRange,,pk2 is multiRange
            sql="select * from "+testTableName+" t where ((t.pk1 >=2 and t.pk1<5) or (t.pk1 >=7 and t.pk1 <9)) and ((t.pk2 >= 4 and t.pk2 <6) or (t.pk2 >= 8 and t.pk2 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(2), true, PInteger.INSTANCE.toBytes(5), false),
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(7), true, PInteger.INSTANCE.toBytes(9), false)
                                ),
                        Arrays.asList(
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(6), false),
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(8), true, PInteger.INSTANCE.toBytes(9), false)
                                )
                            ),
                     rowKeyRanges
                    );
            assertArrayEquals(scan.getStartRow(), ByteUtil.concat(PInteger.INSTANCE.toBytes(2), PInteger.INSTANCE.toBytes(4)));
            assertArrayEquals(scan.getStopRow(), PInteger.INSTANCE.toBytes(9));

            //case4 : only pk1 and pk3, no pk2
            sql="select * from "+testTableName+" t where ((t.pk1 >=2 and t.pk1<5) or (t.pk1 >=7 and t.pk1 <9)) and ((t.pk3 >= 4 and t.pk3 <6) or (t.pk3 >= 8 and t.pk3 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            /**
             * This sql use skipScan, and all the whereExpressions are in SkipScanFilter,
             * so there is no other RowKeyComparisonFilter needed.
             */
            assertTrue(scan.getFilter() instanceof SkipScanFilter);

            rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(2), true, PInteger.INSTANCE.toBytes(5), false),
                                KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(7), true, PInteger.INSTANCE.toBytes(9), false)
                                ),
                       Arrays.asList(KeyRange.EVERYTHING_RANGE),
                       Arrays.asList(
                               KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(6), false),
                               KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(8), true, PInteger.INSTANCE.toBytes(9), false)
                               )
                            ),
                     rowKeyRanges
                    );
            assertArrayEquals(scan.getStartRow(), PInteger.INSTANCE.toBytes(2));
            assertArrayEquals(scan.getStopRow(), PInteger.INSTANCE.toBytes(9));

            //case 5: pk1 or data column
            sql="select * from "+testTableName+" t where ((t.pk1 >=2) or (t.data >= 4 and t.data <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SingleCQKeyValueComparisonFilter);
            Expression pk1Expression =  new ColumnRef(queryPlan.getTableRef(), queryPlan.getTableRef().getTable().getColumnForColumnName("PK1").getPosition()).newColumnExpression();
            Expression dataExpression =  new ColumnRef(queryPlan.getTableRef(), queryPlan.getTableRef().getTable().getColumnForColumnName("DATA").getPosition()).newColumnExpression();
            assertEquals(
                    TestUtil.singleKVFilter(
                        TestUtil.or(
                                TestUtil.constantComparison(CompareOp.GREATER_OR_EQUAL, pk1Expression, 2),
                                TestUtil.and(
                                        TestUtil.constantComparison(CompareOp.GREATER_OR_EQUAL, dataExpression, 4),
                                        TestUtil.constantComparison(CompareOp.LESS, dataExpression, 9)
                                        )
                                )
                    ),
                    scan.getFilter());
            assertArrayEquals(scan.getStartRow(), HConstants.EMPTY_START_ROW);
            assertArrayEquals(scan.getStopRow(), HConstants.EMPTY_END_ROW);

            //case 6: pk1 or pk2,but pk2 is empty range
            sql ="select * from "+testTableName+" t where (t.pk1 >=2 and t.pk1<5) or ((t.pk2 >= 4 and t.pk2 <6) and (t.pk2 >= 8 and t.pk2 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertNull(scan.getFilter());
            assertArrayEquals(scan.getStartRow(), PInteger.INSTANCE.toBytes(2));
            assertArrayEquals(scan.getStopRow(), PInteger.INSTANCE.toBytes(5));

            //case 7: pk1 or pk2,but pk2 is all range
            sql ="select * from "+testTableName+" t where (t.pk1 >=2 and t.pk1<5) or (t.pk2 >=7 or t.pk2 <9)";
            queryPlan= TestUtil.getOptimizeQueryPlan(conn, sql);

            scan = queryPlan.getContext().getScan();
            Expression pk2Expression =  new ColumnRef(queryPlan.getTableRef(), queryPlan.getTableRef().getTable().getColumnForColumnName("PK2").getPosition()).newColumnExpression();
            assertTrue(scan.getFilter() instanceof RowKeyComparisonFilter);
            assertEquals(
                      TestUtil.rowKeyFilter(
                            TestUtil.or(
                                    TestUtil.and(
                                            TestUtil.constantComparison(CompareOp.GREATER_OR_EQUAL,pk1Expression, 2),
                                            TestUtil.constantComparison(CompareOp.LESS,pk1Expression, 5)),
                                    TestUtil.or(
                                            TestUtil.constantComparison(CompareOp.GREATER_OR_EQUAL,pk2Expression, 7),
                                            TestUtil.constantComparison(CompareOp.LESS,pk2Expression, 9))
                                    )
                              ),
                     scan.getFilter());
            assertArrayEquals(scan.getStartRow(), HConstants.EMPTY_START_ROW);
            assertArrayEquals(scan.getStopRow(), HConstants.EMPTY_END_ROW);

            //case 8: pk1 and pk2, but pk1 has a or allRange
            sql="select * from "+testTableName+" t where ((t.pk1 >=2 and t.pk1<5) or (t.pk1 >=7 or t.pk1 <9)) and ((t.pk2 >= 4 and t.pk2 <6) or (t.pk2 >= 8 and t.pk2 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof RowKeyComparisonFilter);
            assertEquals(
                      TestUtil.rowKeyFilter(
                            TestUtil.or(
                                    TestUtil.and(
                                            TestUtil.constantComparison(CompareOp.GREATER_OR_EQUAL,pk2Expression, 4),
                                            TestUtil.constantComparison(CompareOp.LESS,pk2Expression, 6)),
                                    TestUtil.and(
                                            TestUtil.constantComparison(CompareOp.GREATER_OR_EQUAL,pk2Expression, 8),
                                            TestUtil.constantComparison(CompareOp.LESS,pk2Expression, 9))
                                    )
                              ),
                     scan.getFilter());

            assertArrayEquals(scan.getStartRow(), HConstants.EMPTY_START_ROW);
            assertArrayEquals(scan.getStopRow(), HConstants.EMPTY_END_ROW);

            //case 9:  pk1 and pk2, but pk2 has a or allRange
            sql="select * from "+testTableName+" t where ((t.pk1 >= 4 and t.pk1 <6) or (t.pk1 >= 8 and t.pk1 <9)) and ((t.pk2 >=2 and t.pk2<5) or (t.pk2 >=7 or t.pk2 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                            Arrays.asList(
                                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(6), false),
                                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(8), true, PInteger.INSTANCE.toBytes(9), false)
                                    ),
                            Arrays.asList(KeyRange.EVERYTHING_RANGE)),
                    rowKeyRanges);
            assertArrayEquals(scan.getStartRow(), PInteger.INSTANCE.toBytes(4));
            assertArrayEquals(scan.getStopRow(), PInteger.INSTANCE.toBytes(9));

            //case 10: only pk2
            sql = "select * from "+testTableName+" t where (pk2 <=7 or pk2>9)";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            pk2Expression =  new ColumnRef(queryPlan.getTableRef(), queryPlan.getTableRef().getTable().getColumnForColumnName("PK2").getPosition()).newColumnExpression();
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof RowKeyComparisonFilter);
                assertEquals(
                      TestUtil.rowKeyFilter(
                            TestUtil.or(
                                    TestUtil.constantComparison(CompareOp.LESS_OR_EQUAL,pk2Expression, 7),
                                    TestUtil.constantComparison(CompareOp.GREATER,pk2Expression, 9))),
                     scan.getFilter());
            assertArrayEquals(scan.getStartRow(), HConstants.EMPTY_START_ROW);
            assertArrayEquals(scan.getStopRow(), HConstants.EMPTY_END_ROW);
            
            //case 11: pk1 and pk2, but pk1 has a or allRange and force skip scan
            sql="select /*+ SKIP_SCAN */ * from "+testTableName+" t where ((t.pk1 >=2 and t.pk1<5) or (t.pk1 >=7 or t.pk1 <9)) and ((t.pk2 >= 4 and t.pk2 <6) or (t.pk2 >= 8 and t.pk2 <9))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(scan.getFilter())).getSlots();
            assertEquals(
                    Arrays.asList(
                            Arrays.asList(KeyRange.EVERYTHING_RANGE),
                            Arrays.asList(
                                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(4), true, PInteger.INSTANCE.toBytes(6), false),
                                    KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(8), true, PInteger.INSTANCE.toBytes(9), false)
                                    )
                            ),
                    rowKeyRanges);
            assertArrayEquals(scan.getStartRow(), HConstants.EMPTY_START_ROW);
            assertArrayEquals(scan.getStopRow(), HConstants.EMPTY_END_ROW);
        }
        finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }

    @Test
    public void testLastPkColumnIsVariableLengthAndDescBug5307() throws Exception {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String sql =  "CREATE TABLE t1 (\n" +
                    "OBJECT_VERSION VARCHAR NOT NULL,\n" +
                    "LOC VARCHAR,\n" +
                    "CONSTRAINT PK PRIMARY KEY (OBJECT_VERSION DESC))";
            conn.createStatement().execute(sql);

            byte[] startKey = ByteUtil.concat(
                    PVarchar.INSTANCE.toBytes("2222", SortOrder.DESC),
                    QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
            byte[] endKey = ByteUtil.concat(
                    PVarchar.INSTANCE.toBytes("1111", SortOrder.DESC),
                    QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
            ByteUtil.nextKey(endKey, endKey.length);
            sql = "SELECT /*+ RANGE_SCAN */ OBJ.OBJECT_VERSION, OBJ.LOC from t1 AS OBJ "+
                    "where OBJ.OBJECT_VERSION in ('1111','2222')";
            QueryPlan queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            Scan scan = queryPlan.getContext().getScan();
            assertArrayEquals(startKey, scan.getStartRow());
            assertArrayEquals(endKey, scan.getStopRow());

            sql =  "CREATE TABLE t2 (\n" +
                    "OBJECT_ID VARCHAR NOT NULL,\n" +
                    "OBJECT_VERSION VARCHAR NOT NULL,\n" +
                    "LOC VARCHAR,\n" +
                    "CONSTRAINT PK PRIMARY KEY (OBJECT_ID, OBJECT_VERSION DESC))";
            conn.createStatement().execute(sql);

            startKey = ByteUtil.concat(
                    PVarchar.INSTANCE.toBytes("obj1", SortOrder.ASC),
                    QueryConstants.SEPARATOR_BYTE_ARRAY,
                    PVarchar.INSTANCE.toBytes("2222", SortOrder.DESC),
                    QueryConstants.DESC_SEPARATOR_BYTE_ARRAY);
            /**
             * For following sql, queryPlan would use SkipScan and is regarded as PointLookup,
             * so the endKey is computed as {@link SchemaUtil#VAR_BINARY_SCHEMA},see {@link ScanRanges#create}.
             */
            endKey =  ByteUtil.concat(
                    PVarchar.INSTANCE.toBytes("obj3", SortOrder.ASC),
                    QueryConstants.SEPARATOR_BYTE_ARRAY,
                    PVarchar.INSTANCE.toBytes("1111", SortOrder.DESC),
                    QueryConstants.DESC_SEPARATOR_BYTE_ARRAY,
                    QueryConstants.SEPARATOR_BYTE_ARRAY);

            sql = "SELECT OBJ.OBJECT_ID, OBJ.OBJECT_VERSION, OBJ.LOC from t2 AS OBJ "+
                    "where (OBJ.OBJECT_ID, OBJ.OBJECT_VERSION) in (('obj1', '2222'),('obj2', '1111'),('obj3', '1111'))";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            FilterList filterList = (FilterList)scan.getFilter();
            assertTrue(filterList.getOperator() == Operator.MUST_PASS_ALL);
            assertEquals(filterList.getFilters().size(),2);
            assertTrue(filterList.getFilters().get(0) instanceof SkipScanFilter);
            assertTrue(filterList.getFilters().get(1) instanceof RowKeyComparisonFilter);
            RowKeyComparisonFilter rowKeyComparisonFilter =(RowKeyComparisonFilter) filterList.getFilters().get(1);
            assertTrue(rowKeyComparisonFilter.toString().equals(
                    "(OBJECT_ID, OBJECT_VERSION) IN ([111,98,106,49,0,205,205,205,205],[111,98,106,50,0,206,206,206,206],[111,98,106,51,0,206,206,206,206])"));

            assertTrue(queryPlan.getContext().getScanRanges().isPointLookup());
            assertArrayEquals(startKey, scan.getStartRow());
            assertArrayEquals(endKey, scan.getStopRow());
        }
        finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testRVCClipBug5753() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();

            String sql = "CREATE TABLE "+tableName+" (" +
                         " pk1 INTEGER NOT NULL , " +
                         " pk2 INTEGER NOT NULL, " +
                         " pk3 INTEGER NOT NULL, " +
                         " pk4 INTEGER NOT NULL, " +
                         " pk5 INTEGER NOT NULL, " +
                         " pk6 INTEGER NOT NULL, " +
                         " pk7 INTEGER NOT NULL, " +
                         " pk8 INTEGER NOT NULL, " +
                         " v INTEGER, CONSTRAINT PK PRIMARY KEY(pk1,pk2,pk3 desc,pk4,pk5,pk6 desc,pk7,pk8))";;

            stmt.execute(sql);

            List<List<KeyRange>> rowKeyRanges = null;
            RowKeyComparisonFilter rowKeyComparisonFilter = null;
            QueryPlan queryPlan = null;
            Scan scan = null;

            sql = "SELECT  /*+ RANGE_SCAN */ * FROM  "+ tableName +
                  " WHERE (pk1, pk2) IN ((2, 3), (2, 4)) AND pk3 = 5";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof RowKeyComparisonFilter);
            rowKeyComparisonFilter = (RowKeyComparisonFilter)scan.getFilter();
            assertTrue(rowKeyComparisonFilter.toString().equals(
                    "((PK1, PK2) IN ([128,0,0,2,128,0,0,3],[128,0,0,2,128,0,0,4]) AND PK3 = 5)"));
            assertArrayEquals(
                    scan.getStartRow(),
                    ByteUtil.concat(
                            PInteger.INSTANCE.toBytes(2),
                            PInteger.INSTANCE.toBytes(3),
                            PInteger.INSTANCE.toBytes(5, SortOrder.DESC)));
            assertArrayEquals(
                    scan.getStopRow(),
                    ByteUtil.concat(
                            PInteger.INSTANCE.toBytes(2),
                            PInteger.INSTANCE.toBytes(4),
                            ByteUtil.nextKey(PInteger.INSTANCE.toBytes(5, SortOrder.DESC))));

            sql = "select * from " + tableName +
                    " where (pk1 >=1 and pk1<=2) and (pk2>=2 and pk2<=3) and (pk3,pk4) < (3,5)";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof FilterList);
            FilterList filterList = (FilterList)scan.getFilter();

            assertTrue(filterList.getOperator() == Operator.MUST_PASS_ALL);
            assertEquals(filterList.getFilters().size(),2);
            assertTrue(filterList.getFilters().get(0) instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(filterList.getFilters().get(0))).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(1),
                                    true,
                                    PInteger.INSTANCE.toBytes(2),
                                    true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(2),
                                    true,
                                    PInteger.INSTANCE.toBytes(3),
                                    true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(3, SortOrder.DESC),
                                    true,
                                    KeyRange.UNBOUND,
                                    false))
                    ),
                    rowKeyRanges);
            assertArrayEquals(
                    scan.getStartRow(),
                    ByteUtil.concat(
                            PInteger.INSTANCE.toBytes(1),
                            PInteger.INSTANCE.toBytes(2),
                            PInteger.INSTANCE.toBytes(3, SortOrder.DESC)));
            assertArrayEquals(
                    scan.getStopRow(),
                    ByteUtil.concat(
                    PInteger.INSTANCE.toBytes(2),
                    PInteger.INSTANCE.toBytes(4)));

            assertTrue(filterList.getFilters().get(1) instanceof RowKeyComparisonFilter);
            rowKeyComparisonFilter =(RowKeyComparisonFilter) filterList.getFilters().get(1);
            assertTrue(rowKeyComparisonFilter.toString().equals(
                    "(TO_INTEGER(PK3), PK4) < (TO_INTEGER(TO_INTEGER(3)), 5)"));

            /**
             * RVC is singleKey
             */
            sql = "select * from " + tableName +
                    " where (pk1 >=1 and pk1<=2) and (pk2>=2 and pk2<=3) and (pk3,pk4) in ((3,4),(4,5)) and "+
                    " (pk5,pk6,pk7) in ((5,6,7),(6,7,8)) and pk8 > 8";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof FilterList);
            filterList = (FilterList)scan.getFilter();

            assertTrue(filterList.getOperator() == Operator.MUST_PASS_ALL);
            assertEquals(filterList.getFilters().size(),2);
            assertTrue(filterList.getFilters().get(0) instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(filterList.getFilters().get(0))).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(1),
                                        true,
                                        PInteger.INSTANCE.toBytes(2),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(2),
                                        true,
                                        PInteger.INSTANCE.toBytes(3),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(4, SortOrder.DESC),
                                        true,
                                        PInteger.INSTANCE.toBytes(4, SortOrder.DESC),
                                        true),
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(3, SortOrder.DESC),
                                        true,
                                        PInteger.INSTANCE.toBytes(3, SortOrder.DESC),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(4),
                                        true,
                                        PInteger.INSTANCE.toBytes(4),
                                        true),
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(5),
                                        true,
                                        PInteger.INSTANCE.toBytes(5),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(5),
                                        true,
                                        PInteger.INSTANCE.toBytes(5),
                                        true),
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(6),
                                        true,
                                        PInteger.INSTANCE.toBytes(6),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(7, SortOrder.DESC),
                                        true,
                                        PInteger.INSTANCE.toBytes(7, SortOrder.DESC),
                                        true),
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(6, SortOrder.DESC),
                                        true,
                                        PInteger.INSTANCE.toBytes(6, SortOrder.DESC),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(7),
                                        true,
                                        PInteger.INSTANCE.toBytes(7),
                                        true),
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(8),
                                        true,
                                        PInteger.INSTANCE.toBytes(8),
                                        true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                        PInteger.INSTANCE.toBytes(9),
                                        true,
                                        KeyRange.UNBOUND,
                                        false))
                    ),
                    rowKeyRanges);
            assertArrayEquals(
                    scan.getStartRow(),
                    ByteUtil.concat(
                            PInteger.INSTANCE.toBytes(1),
                            PInteger.INSTANCE.toBytes(2),
                            PInteger.INSTANCE.toBytes(4, SortOrder.DESC),
                            PInteger.INSTANCE.toBytes(4),
                            PInteger.INSTANCE.toBytes(5),
                            PInteger.INSTANCE.toBytes(7, SortOrder.DESC),
                            PInteger.INSTANCE.toBytes(7),
                            PInteger.INSTANCE.toBytes(9)));
            assertArrayEquals(
                    scan.getStopRow(),
                    ByteUtil.concat(
                    PInteger.INSTANCE.toBytes(2),
                    PInteger.INSTANCE.toBytes(3),
                    PInteger.INSTANCE.toBytes(3, SortOrder.DESC),
                    PInteger.INSTANCE.toBytes(5),
                    PInteger.INSTANCE.toBytes(6),
                    PInteger.INSTANCE.toBytes(6, SortOrder.DESC),
                    PInteger.INSTANCE.toBytes(9)));

            assertTrue(filterList.getFilters().get(1) instanceof RowKeyComparisonFilter);
            rowKeyComparisonFilter =(RowKeyComparisonFilter) filterList.getFilters().get(1);
            assertTrue(rowKeyComparisonFilter.toString().equals(
                    "((PK3, PK4) IN ([127,255,255,251,128,0,0,5],[127,255,255,252,128,0,0,4])"+
                    " AND (PK5, PK6, PK7) IN ([128,0,0,5,127,255,255,249,128,0,0,7],[128,0,0,6,127,255,255,248,128,0,0,8]))"));
            /**
             * RVC is not singleKey
             */
            sql = "select * from " + tableName +
                  " where (pk1 >=1 and pk1<=2) and (pk2>=2 and pk2<=3) and (pk3,pk4) < (3,4) and "+
                  " (pk5,pk6,pk7) < (5,6,7) and pk8 > 8";
            queryPlan = TestUtil.getOptimizeQueryPlan(conn, sql);
            scan = queryPlan.getContext().getScan();
            assertTrue(scan.getFilter() instanceof FilterList);
            filterList = (FilterList)scan.getFilter();

            assertTrue(filterList.getOperator() == Operator.MUST_PASS_ALL);
            assertEquals(filterList.getFilters().size(),2);
            assertTrue(filterList.getFilters().get(0) instanceof SkipScanFilter);
            rowKeyRanges = ((SkipScanFilter)(filterList.getFilters().get(0))).getSlots();
            assertEquals(
                    Arrays.asList(
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(1),
                                    true,
                                    PInteger.INSTANCE.toBytes(2),
                                    true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(2),
                                    true,
                                    PInteger.INSTANCE.toBytes(3),
                                    true)),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(3, SortOrder.DESC),
                                    true,
                                    KeyRange.UNBOUND,
                                    false)),
                        Arrays.asList(KeyRange.EVERYTHING_RANGE),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    KeyRange.UNBOUND,
                                    false,
                                    PInteger.INSTANCE.toBytes(5),
                                    true)),
                        Arrays.asList(KeyRange.EVERYTHING_RANGE),
                        Arrays.asList(KeyRange.EVERYTHING_RANGE),
                        Arrays.asList(
                                KeyRange.getKeyRange(
                                    PInteger.INSTANCE.toBytes(9),
                                    true,
                                    KeyRange.UNBOUND,
                                    false))
                    ),
                    rowKeyRanges);
            assertArrayEquals(
                    scan.getStartRow(),
                    ByteUtil.concat(
                            PInteger.INSTANCE.toBytes(1),
                            PInteger.INSTANCE.toBytes(2),
                            PInteger.INSTANCE.toBytes(3, SortOrder.DESC)));
            assertArrayEquals(
                    scan.getStopRow(),
                    ByteUtil.concat(
                    PInteger.INSTANCE.toBytes(2),
                    PInteger.INSTANCE.toBytes(4)));

            assertTrue(filterList.getFilters().get(1) instanceof RowKeyComparisonFilter);
            rowKeyComparisonFilter =(RowKeyComparisonFilter) filterList.getFilters().get(1);
            assertTrue(rowKeyComparisonFilter.toString().equals(
                    "((TO_INTEGER(PK3), PK4) < (TO_INTEGER(TO_INTEGER(3)), 4) AND "+
                    "(PK5, TO_INTEGER(PK6), PK7) < (5, TO_INTEGER(TO_INTEGER(6)), 7))"));
        }
    }


    @Test
    public void testWithLargeORs() throws Exception {

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

        String tableName = generateUniqueName();
        String viewName = String.format("Z_%s", tableName);
        PDataType[] testTSVarVarPKTypes = new PDataType[] { PTimestamp.INSTANCE, PVarchar.INSTANCE, PInteger.INSTANCE};
        String baseTableName = String.format("TEST_ENTITY.%s", tableName);
        int tenantId = 1;
        int numTestCases = 1;
        for (int index=0;index<sortOrders.length;index++) {
            // Test Case 1: PK1 = Timestamp, PK2 = Varchar, PK3 = Integer
            String view1Name = String.format("TEST_ENTITY.%s%d",
                    viewName, index*numTestCases+1);
            String partition1 = String.format("Z%d", index*numTestCases+1);
            createTenantView(tenantId,
                    baseTableName, view1Name, partition1,
                    testTSVarVarPKTypes[0], sortOrders[index][0],
                    testTSVarVarPKTypes[1], sortOrders[index][1],
                    testTSVarVarPKTypes[2], sortOrders[index][2]);
            testTSVarIntAndLargeORs(tenantId, view1Name, sortOrders[index]);
        }
    }

    private void createBaseTable(String baseTable) throws SQLException {

        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            try (Statement cstmt = globalConnection.createStatement()) {
                String CO_BASE_TBL_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s(OID CHAR(15) NOT NULL,KP CHAR(3) NOT NULL,ROW_ID VARCHAR, COL1 VARCHAR,COL2 VARCHAR,COL3 VARCHAR,CREATED_DATE DATE,CREATED_BY CHAR(15),LAST_UPDATE DATE,LAST_UPDATE_BY CHAR(15),SYSTEM_MODSTAMP DATE CONSTRAINT pk PRIMARY KEY (OID,KP)) MULTI_TENANT=true,COLUMN_ENCODED_BYTES=0";
                cstmt.execute(String.format(CO_BASE_TBL_TEMPLATE, baseTable));
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
    // Test Case 1: PK1 = Timestamp, PK2 = Varchar, PK3 = Integer
    private void testTSVarIntAndLargeORs(int tenantId, String viewName, SortOrder[] sortOrder) throws SQLException {
        String testName = "testLargeORs";
        String testLargeORs = String.format("SELECT ROW_ID FROM %s ", viewName);

        PDataType[] testPKTypes = new PDataType[] { PTimestamp.INSTANCE, PVarchar.INSTANCE, PInteger.INSTANCE};
        assertExpectedWithMaxInListAndLargeORs(tenantId, testName, testPKTypes, testLargeORs, sortOrder);

    }

    public void assertExpectedWithMaxInListAndLargeORs(int tenantId,  String testType, PDataType[] testPKTypes, String testSQL, SortOrder[] sortOrder) throws SQLException {

        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        int numINs = 25;
        int expectedExtractedNodes = Arrays.asList(new SortOrder[] {sortOrder[0], sortOrder[1]}).stream().allMatch(Predicate.isEqual(SortOrder.ASC)) ? 3 : 2;

        // Test for increasing orders of ORs (5,50,500,5000)
        for (int o = 0; o < 4;o++) {
            int numORs = (int) (5.0 * Math.pow(10.0,(double) o));
            String context = "ORs:" + numORs + ", sql: " + testSQL + ", type: " + testType + ", sort-order: " + Arrays.stream(sortOrder).map(s -> s.name()).collect(Collectors.joining(","));
            String tenantConnectionUrl = String.format("%s;%s=%s%06d", getUrl(), TENANT_ID_ATTRIB, TENANT_PREFIX, tenantId);
            try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl, tenantProps)) {
                // Generate the where clause
                StringBuilder whereClause = new StringBuilder("(ID1,ID2) IN ((?,?)");
                for (int i = 0; i < numINs; i++) {
                    whereClause.append(",(?,?)");
                }
                whereClause.append(") AND (ID3 = ? ");
                for (int i = 0; i < numORs; i++) {
                    whereClause.append(" OR ID3 = ?");
                }
                whereClause.append(") LIMIT 200");
                // Full SQL
                String query = testSQL + " WHERE " + whereClause;

                PhoenixPreparedStatement stmtForExtractNodesCheck =
                        tenantConnection.prepareStatement(query).unwrap(PhoenixPreparedStatement.class);
                int lastBoundCol = 0;
                lastBoundCol = setBindVariables(stmtForExtractNodesCheck, lastBoundCol,
                        numINs + 1,
                        new PDataType[] {testPKTypes[0], testPKTypes[1]});
                lastBoundCol = setBindVariables(stmtForExtractNodesCheck, lastBoundCol,
                        numORs + 1,  new PDataType[] {testPKTypes[2]});

                // Get the column resolver
                SelectStatement selectStatement = new SQLParser(query).parseQuery();
                ColumnResolver resolver = FromCompiler.getResolverForQuery(selectStatement,
                        tenantConnection.unwrap(PhoenixConnection.class));

                // Where clause with INs and ORs
                ParseNode whereNode = selectStatement.getWhere();
                Expression whereExpression = whereNode.accept(new TestWhereExpressionCompiler(
                        new StatementContext(stmtForExtractNodesCheck, resolver)));

                // Tenant view where clause
                ParseNode viewWhere = SQLParser.parseCondition("KP = 'ECZ'");
                Expression viewWhereExpression = viewWhere.accept(new TestWhereExpressionCompiler(
                        new StatementContext(stmtForExtractNodesCheck, resolver)));

                // Build the test expression
                Expression testExpression = AndExpression.create(
                        Lists.newArrayList(whereExpression, viewWhereExpression));

                // Test
                Set<Expression> extractedNodes = Sets.<Expression>newHashSet();
                WhereOptimizer.pushKeyExpressionsToScan(new StatementContext(stmtForExtractNodesCheck, resolver),
                        Collections.emptySet(), testExpression, extractedNodes, Optional.<byte[]>absent());
                assertEquals(String.format("Unexpected results expected = %d, actual = %d extracted nodes",
                                expectedExtractedNodes, extractedNodes.size()),
                        expectedExtractedNodes, extractedNodes.size());
            }

        }
    }

}
