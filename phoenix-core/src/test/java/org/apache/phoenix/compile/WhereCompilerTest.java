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

import static org.apache.phoenix.compile.WhereCompiler.transformDNF;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.and;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.apache.phoenix.util.TestUtil.bindParams;
import static org.apache.phoenix.util.TestUtil.columnComparison;
import static org.apache.phoenix.util.TestUtil.constantComparison;
import static org.apache.phoenix.util.TestUtil.in;
import static org.apache.phoenix.util.TestUtil.multiEncodedKVFilter;
import static org.apache.phoenix.util.TestUtil.not;
import static org.apache.phoenix.util.TestUtil.or;
import static org.apache.phoenix.util.TestUtil.singleKVFilter;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;


public class WhereCompilerTest extends BaseConnectionlessQueryTest {

    private PhoenixPreparedStatement newPreparedStatement(PhoenixConnection pconn, String query) throws SQLException {
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        assertRoundtrip(query);
        return pstmt;
    }
    
    @Test
    public void testSingleEqualFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOperator.EQUAL,
                A_INTEGER,
                0)),
            filter);
    }

    @Test
    public void testOrPKWithAndPKAndNotPK() throws SQLException {
        String query = "select * from bugTable where ID = 'i1' or (ID = 'i2' and company = 'c3')";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("create table bugTable(ID varchar primary key,company varchar)");
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        Expression idExpression = new ColumnRef(plan.getTableRef(), plan.getTableRef().getTable().getColumnForColumnName("ID").getPosition()).newColumnExpression();
        Expression id = new RowKeyColumnExpression(idExpression,new RowKeyValueAccessor(plan.getTableRef().getTable().getPKColumns(),0));
        Expression company = new KeyValueColumnExpression(plan.getTableRef().getTable().getColumnForColumnName("COMPANY"));
        // FilterList has no equals implementation
        assertTrue(filter instanceof FilterList);
        FilterList filterList = (FilterList)filter;
        assertEquals(FilterList.Operator.MUST_PASS_ALL, filterList.getOperator());
        assertEquals(
            Arrays.asList(
                new SkipScanFilter(
                    ImmutableList.of(Arrays.asList(
                        pointRange("i1"),
                        pointRange("i2"))),
                    SchemaUtil.VAR_BINARY_SCHEMA, false),
                singleKVFilter(
                        or(constantComparison(CompareOperator.EQUAL,id,"i1"),
                           and(constantComparison(CompareOperator.EQUAL,id,"i2"),
                               constantComparison(CompareOperator.EQUAL,company,"c3"))))),
            filterList.getFilters());
    }

    @Test
    public void testAndPKAndNotPK() throws SQLException {
        String query = "select * from bugTable where ID = 'i2' and company = 'c3'";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("create table bugTable(ID varchar primary key,company varchar)");
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        PColumn column = plan.getTableRef().getTable().getColumnForColumnName("COMPANY");
        assertEquals(
                singleKVFilter(constantComparison(
                        CompareOperator.EQUAL,
                        new KeyValueColumnExpression(column),
                        "c3")),
                filter);
    }

    @Test
    public void testSingleFixedFullPkSalted() throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("CREATE TABLE t (k bigint not null primary key, v varchar) SALT_BUCKETS=20");
        String query = "select * from t where k=" + 1;
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] key = new byte[PLong.INSTANCE.getByteSize() + 1];
        PLong.INSTANCE.toBytes(1L, key, 1);
        key[0] = SaltingUtil.getSaltingByte(key, 1, PLong.INSTANCE.getByteSize(), 20);
        byte[] expectedStartKey = key;
        byte[] expectedEndKey = ByteUtil.nextKey(key);
        byte[] startKey = scan.getStartRow();
        byte[] stopKey = scan.getStopRow();
        assertArrayEquals(expectedStartKey, startKey);
        assertArrayEquals(expectedEndKey, stopKey);
    }

    @Test
    public void testSingleVariableFullPkSalted() throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("CREATE TABLE t (k varchar(10) primary key, v varchar) SALT_BUCKETS=20");
        String query = "select * from t where k='a'";
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] key = new byte[2];
        PVarchar.INSTANCE.toBytes("a", key, 1);
        key[0] = SaltingUtil.getSaltingByte(key, 1, 1, 20);
        byte[] expectedStartKey = key;
        //lexicographically this is the next PK
        byte[] expectedEndKey = ByteUtil.concat(key,new byte[]{0});
        byte[] startKey = scan.getStartRow();
        byte[] stopKey = scan.getStopRow();
        assertTrue(Bytes.compareTo(expectedStartKey, startKey) == 0);
        assertTrue(Bytes.compareTo(expectedEndKey, stopKey) == 0);
    }

    @Test
    public void testMultiFixedFullPkSalted() throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("CREATE TABLE t (k bigint not null primary key, v varchar) SALT_BUCKETS=20");
        String query = "select * from t where k in (1,3)";
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        byte[] key = new byte[PLong.INSTANCE.getByteSize() + 1];
        PLong.INSTANCE.toBytes(1L, key, 1);
        key[0] = SaltingUtil.getSaltingByte(key, 1, PLong.INSTANCE.getByteSize(), 20);
        byte[] startKey1 = key;

        key = new byte[PLong.INSTANCE.getByteSize() + 1];
        PLong.INSTANCE.toBytes(3L, key, 1);
        key[0] = SaltingUtil.getSaltingByte(key, 1, PLong.INSTANCE.getByteSize(), 20);
        byte[] startKey2 = key;

        byte[] startKey = scan.getStartRow();
        byte[] stopKey = scan.getStopRow();

        // Due to salting byte, the 1 key may be after the 3 key
        byte[] expectedStartKey;
        byte[] expectedEndKey;
        List<List<KeyRange>> expectedRanges = Collections.singletonList(
                Arrays.asList(KeyRange.getKeyRange(startKey1),
                              KeyRange.getKeyRange(startKey2)));
        if (Bytes.compareTo(startKey1, startKey2) > 0) {
            expectedStartKey = startKey2;
            expectedEndKey = startKey1;
            Collections.reverse(expectedRanges.get(0));
        } else {
            expectedStartKey = startKey1;
            expectedEndKey = startKey2;
        }
        assertEquals(0,startKey.length);
        assertEquals(0,stopKey.length);

        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        SkipScanFilter skipScanFilter = (SkipScanFilter)filter;
        assertEquals(1,skipScanFilter.getSlots().size());
        assertEquals(2,skipScanFilter.getSlots().get(0).size());
        assertArrayEquals(expectedStartKey, skipScanFilter.getSlots().get(0).get(0).getLowerRange());
        assertArrayEquals(expectedEndKey, skipScanFilter.getSlots().get(0).get(1).getLowerRange());
        StatementContext context = plan.getContext();
        ScanRanges scanRanges = context.getScanRanges();
        List<List<KeyRange>> ranges = scanRanges.getRanges();
        assertEquals(expectedRanges, ranges);
    }

    @Test
    public void testMultiColumnEqualFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_string=b_string";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
                multiEncodedKVFilter(columnComparison(
                        CompareOperator.EQUAL,
                        A_STRING,
                        B_STRING), TWO_BYTE_QUALIFIERS),
                filter);
    }

    @Test
    public void testCollapseFunctionToNull() throws SQLException {
        String query = "select * from atable where substr(entity_id,null) = 'foo'";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);

        assertArrayEquals(scan.getStartRow(),KeyRange.EMPTY_RANGE.getLowerRange());
        assertArrayEquals(scan.getStopRow(),KeyRange.EMPTY_RANGE.getUpperRange());
    }

    @Test
    public void testAndFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id=? and a_integer=0 and a_string='foo'";
        List<Object> binds = Arrays.<Object>asList(tenantId);

        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();

        assertEquals(
                multiEncodedKVFilter(and(
                        constantComparison(
                                CompareOperator.EQUAL,
                                A_INTEGER,
                                0),
                        constantComparison(
                                CompareOperator.EQUAL,
                                A_STRING,
                                "foo")), TWO_BYTE_QUALIFIERS),
                filter);
    }

    @Test
    public void testRHSLiteral() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 0 >= a_integer";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();

        Filter filter = scan.getFilter();
        assertEquals(
                singleKVFilter(constantComparison(
                        CompareOperator.LESS_OR_EQUAL,
                        A_INTEGER,
                        0)),
                filter);
    }

    @Test
    public void testToDateFilter() throws Exception {
        String tenantId = "000000000000001";
        String dateStr = "2012-01-01 12:00:00";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_date >= to_date('" + dateStr + "')";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();

        Date date = DateUtil.parseDate(dateStr);

        assertEquals(
            singleKVFilter(constantComparison(
                    CompareOperator.GREATER_OR_EQUAL,
                A_DATE,
                date)),
            filter);
    }

    private void helpTestToNumberFilter(String toNumberClause, BigDecimal expectedDecimal) throws Exception {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and x_decimal >= " + toNumberClause;
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();

        assertEquals(
            singleKVFilter(constantComparison(
                    CompareOperator.GREATER_OR_EQUAL,
                X_DECIMAL,
                expectedDecimal)),
            filter);
}

    private void helpTestToNumberFilterWithNoPattern(String stringValue) throws Exception {
        String toNumberClause = "to_number('" + stringValue + "')";
        BigDecimal expectedDecimal = NumberUtil.normalize(new BigDecimal(stringValue));
        helpTestToNumberFilter(toNumberClause, expectedDecimal);
    }

    @Test
    public void testToNumberFilterWithInteger() throws Exception {
        String stringValue = "123";
        helpTestToNumberFilterWithNoPattern(stringValue);
    }

    @Test
    public void testToNumberFilterWithDecimal() throws Exception {
        String stringValue = "123.33";
        helpTestToNumberFilterWithNoPattern(stringValue);
    }

    @Test
    public void testToNumberFilterWithNegativeDecimal() throws Exception {
        String stringValue = "-123.33";
        helpTestToNumberFilterWithNoPattern(stringValue);
    }

    @Test
    public void testToNumberFilterWithPatternParam() throws Exception {
        String toNumberClause = "to_number('!1.23333E2', '!0.00000E0')";
        BigDecimal expectedDecimal = NumberUtil.normalize(new BigDecimal("123.333"));
        helpTestToNumberFilter(toNumberClause, expectedDecimal);
    }

    @Test(expected=AssertionError.class) // compileStatement() fails because zero rows are found by to_number()
    public void testToNumberFilterWithPatternParamNegativeTest() throws Exception {
        String toNumberClause = "to_number('$123.33', '000.00')"; // no currency sign in pattern param
        BigDecimal expectedDecimal = NumberUtil.normalize(new BigDecimal("123.33"));
        helpTestToNumberFilter(toNumberClause, expectedDecimal);
    }

    @Test
    public void testRowKeyFilter() throws SQLException {
        String keyPrefix = "foo";
        String query = "select * from atable where substr(entity_id,1,3)=?";
        List<Object> binds = Arrays.<Object>asList(keyPrefix);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();

        assertEquals(
            new RowKeyComparisonFilter(
                constantComparison(CompareOperator.EQUAL,
                    new SubstrFunction(
                        Arrays.<Expression>asList(
                            new RowKeyColumnExpression(ENTITY_ID,new RowKeyValueAccessor(ATABLE.getPKColumns(),1)),
                            LiteralExpression.newConstant(1),
                            LiteralExpression.newConstant(3))
                        ),
                    keyPrefix),
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES),
            filter);
    }

    @Test
    public void testPaddedRowKeyFilter() throws SQLException {
        String keyPrefix = "fo";
        String query = "select * from atable where entity_id=?";
        List<Object> binds = Arrays.<Object>asList(keyPrefix);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        assertEquals(0,scan.getStartRow().length);
        assertEquals(0,scan.getStopRow().length);
        assertNotNull(scan.getFilter());
    }

    @Test
    public void testPaddedStartStopKey() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "fo";
        String query = "select * from atable where organization_id=? AND entity_id=?";
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        byte[] expectedStartRow = ByteUtil.concat(Bytes.toBytes(tenantId), StringUtil.padChar(Bytes.toBytes(keyPrefix), 15));
        assertArrayEquals(expectedStartRow,scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(expectedStartRow),scan.getStopRow());
    }

    @Test
    public void testDegenerateRowKeyFilter() throws SQLException {
        String keyPrefix = "foobar";
        String query = "select * from atable where substr(entity_id,1,3)=?";
        List<Object> binds = Arrays.<Object>asList(keyPrefix);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        // Degenerate b/c "foobar" is more than 3 characters
        assertDegenerate(plan.getContext());
    }

    @Test
    public void testDegenerateBiggerThanMaxLengthVarchar() throws SQLException {
        byte[] tooBigValue = new byte[101];
        Arrays.fill(tooBigValue, (byte)50);
        String aString = (String) PVarchar.INSTANCE.toObject(tooBigValue);
        String query = "select * from atable where a_string=?";
        List<Object> binds = Arrays.<Object>asList(aString);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        // Degenerate b/c a_string length is 100
        assertDegenerate(plan.getContext());
    }

    @Test
    public void testOrFilter() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "foo";
        int aInt = 2;
        String query = "select * from atable where organization_id=? and (substr(entity_id,1,3)=? or a_integer=?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, keyPrefix, aInt);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        bindParams(pstmt, binds);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter( // single b/c one column is a row key column
            or(
                constantComparison(
                    CompareOperator.EQUAL,
                    new SubstrFunction(Arrays.<Expression> asList(
                        new RowKeyColumnExpression(
                            ENTITY_ID,
                            new RowKeyValueAccessor(ATABLE.getPKColumns(), 1)),
                        LiteralExpression.newConstant(1),
                        LiteralExpression.newConstant(3))),
                    keyPrefix),
                constantComparison(
                    CompareOperator.EQUAL,
                    A_INTEGER,
                    aInt))),
            filter);
    }

    @Test
    public void testTypeMismatch() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer > 'foo'";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);

        try {
            pstmt.optimizeQuery();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testAndFalseFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and 2=3";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        assertDegenerate(plan.getContext());
    }

    @Test
    public void testFalseFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 2=3";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        assertDegenerate(plan.getContext());
    }

    @Test
    public void testTrueFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 2<=2";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        assertNull(scan.getFilter());
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testAndTrueFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and 2<3";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOperator.EQUAL,
                A_INTEGER,
                0)),
            filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testOrFalseFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and (a_integer=0 or 3!=3)";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(constantComparison(
                CompareOperator.EQUAL,
                A_INTEGER,
                0)),
            filter);
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testOrTrueFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and (a_integer=0 or 3>2)";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertNull(filter);
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testInFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_string IN ('a','b')";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());

        Filter filter = scan.getFilter();
        assertEquals(
            singleKVFilter(in(
                A_STRING,
                "a",
                "b")),
            filter);
    }

    @Test
    public void testInListFilter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s')",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = PVarchar.INSTANCE.toBytes(tenantId3);
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());

        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(Arrays.asList(
                    pointRange(tenantId1),
                    pointRange(tenantId2),
                    pointRange(tenantId3))),
                plan.getTableRef().getTable().getRowKeySchema(), false),
            filter);
    }

    @Test @Ignore("OR not yet optimized")
    public void testOr2InFilter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String query = String.format("select * from %s where organization_id='%s' OR organization_id='%s' OR organization_id='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();

        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(Arrays.asList(
                    pointRange(tenantId1),
                    pointRange(tenantId2),
                    pointRange(tenantId3))),
                plan.getTableRef().getTable().getRowKeySchema(), false),
            filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = PVarchar.INSTANCE.toBytes(tenantId3);
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testSecondPkColInListFilter() throws SQLException {
        String tenantId = "000000000000001";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id='%s' AND entity_id IN ('%s','%s')",
                ATABLE_NAME, tenantId, entityId1, entityId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId + entityId1);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = PVarchar.INSTANCE.toBytes(tenantId + entityId2);
        assertArrayEquals(ByteUtil.concat(stopRow, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());

        Filter filter = scan.getFilter();

        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(
                        pointRange(tenantId,entityId1),
                        pointRange(tenantId,entityId2))),
                SchemaUtil.VAR_BINARY_SCHEMA, false),
            filter);
    }

    @Test
    public void testInListWithAnd1GTEFilter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id>='%s' AND entity_id<='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId1, entityId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(
                        pointRange(tenantId1),
                        pointRange(tenantId2),
                        pointRange(tenantId3)),
                    Arrays.asList(PChar.INSTANCE.getKeyRange(
                        Bytes.toBytes(entityId1),
                        true,
                        Bytes.toBytes(entityId2),
                        true, SortOrder.ASC))),
                plan.getTableRef().getTable().getRowKeySchema(), false),
            filter);
    }

    @Test
    public void testInListWithAnd1Filter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId = "00000000000000X";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                ImmutableList.of(
                    Arrays.asList(
                        pointRange(tenantId1, entityId),
                        pointRange(tenantId2, entityId),
                        pointRange(tenantId3, entityId))),
                SchemaUtil.VAR_BINARY_SCHEMA, false),
            filter);
    }
    @Test
    public void testInListWithAnd1FilterScankey() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId = "00000000000000X";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id='%s'",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId1), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId3), PVarchar.INSTANCE.toBytes(entityId));
        assertArrayEquals(ByteUtil.concat(stopRow, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
        // TODO: validate scan ranges
    }

    private static KeyRange pointRange(String... ids) {
        byte[] theKey = ByteUtil.EMPTY_BYTE_ARRAY;
        for (String id : ids) {
            theKey = ByteUtil.concat(theKey, Bytes.toBytes(id));
        }
        return pointRange(theKey);
    }
    private static KeyRange pointRange(byte[] bytes) {
        return KeyRange.POINT.apply(bytes);
    }

    @Test
    public void testInListWithAnd2Filter() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id IN ('%s','%s') AND entity_id IN ('%s', '%s')",
                ATABLE_NAME, tenantId1, tenantId2, entityId1, entityId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();

        Filter filter = scan.getFilter();
        assertEquals(
            new SkipScanFilter(
                    ImmutableList.<List<KeyRange>>of(ImmutableList.of(
                        pointRange(tenantId1, entityId1),
                        pointRange(tenantId1, entityId2),
                        pointRange(tenantId2, entityId1),
                        pointRange(tenantId2, entityId2))),
                SchemaUtil.VAR_BINARY_SCHEMA, false),
            filter);
    }

    @Test
    public void testPartialRangeFilter() throws SQLException {
        // I know these id's are ridiculous, but users can write queries that look like this
        String tenantId1 = "001";
        String tenantId2 = "02";
        String query = String.format("select * from %s where organization_id > '%s' AND organization_id < '%s'",
                ATABLE_NAME, tenantId1, tenantId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();

        assertNull(scan.getFilter());
        byte[] wideLower = ByteUtil.nextKey(StringUtil.padChar(Bytes.toBytes(tenantId1), 15));
        byte[] wideUpper = StringUtil.padChar(Bytes.toBytes(tenantId2), 15);
        assertArrayEquals(wideLower, scan.getStartRow());
        assertArrayEquals(wideUpper, scan.getStopRow());
    }

    @Test
    public void testInListWithAnd2FilterScanKey() throws SQLException {
        String tenantId1 = "000000000000001";
        String tenantId2 = "000000000000002";
        String tenantId3 = "000000000000003";
        String entityId1 = "00000000000000X";
        String entityId2 = "00000000000000Y";
        String query = String.format("select * from %s where organization_id IN ('%s','%s','%s') AND entity_id IN ('%s', '%s')",
                ATABLE_NAME, tenantId1, tenantId3, tenantId2, entityId1, entityId2);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        byte[] startRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId1), PVarchar.INSTANCE.toBytes(entityId1));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId3), PVarchar.INSTANCE.toBytes(entityId2));
        assertArrayEquals(ByteUtil.concat(stopRow, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
        // TODO: validate scan ranges
    }

    @Test
    public void testBetweenFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer between 0 and 10";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
                singleKVFilter(and(
                    constantComparison(
                        CompareOperator.GREATER_OR_EQUAL,
                        A_INTEGER,
                        0),
                    constantComparison(
                        CompareOperator.LESS_OR_EQUAL,
                        A_INTEGER,
                        10))),
                filter);
    }

    @Test
    public void testNotBetweenFilter() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer not between 0 and 10";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        assertEquals(
                singleKVFilter(not(and(
                    constantComparison(
                        CompareOperator.GREATER_OR_EQUAL,
                        A_INTEGER,
                        0),
                    constantComparison(
                        CompareOperator.LESS_OR_EQUAL,
                        A_INTEGER,
                        10)))).toString(),
                filter.toString());
    }

    @Test
    public void testTenantConstraintsAddedToScan() throws SQLException {
        String tenantTypeId = "5678";
        String tenantId = "000000000000123";
        String url = getUrl(tenantId);
        createTestTable(getUrl(), "create table base_table_for_tenant_filter_test (tenant_id char(15) not null, type_id char(4) not null, " +
        		"id char(5) not null, a_integer integer, a_string varchar(100) constraint pk primary key (tenant_id, type_id, id)) multi_tenant=true");
        createTestTable(url, "create view tenant_filter_test (tenant_col integer) AS SELECT * FROM BASE_TABLE_FOR_TENANT_FILTER_TEST WHERE type_id= '" + tenantTypeId + "'");

        String query = "select * from tenant_filter_test where a_integer=0 and a_string='foo'";
        PhoenixConnection pconn = DriverManager.getConnection(url, PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        PTable table = plan.getTableRef().getTable();
        Expression aInteger = new ColumnRef(new TableRef(table), table.getColumnForColumnName("A_INTEGER").getPosition()).newColumnExpression();
        Expression aString = new ColumnRef(new TableRef(table), table.getColumnForColumnName("A_STRING").getPosition()).newColumnExpression();
        assertEquals(
            multiEncodedKVFilter(and(
                constantComparison(
                    CompareOperator.EQUAL,
                    aInteger,
                    0),
                constantComparison(
                    CompareOperator.EQUAL,
                    aString,
                    "foo")), TWO_BYTE_QUALIFIERS),
            filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId + tenantTypeId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testTenantConstraintsAddedToScanWithNullTenantTypeId() throws SQLException {
        String tenantId = "000000000000123";
        createTestTable(getUrl(), "create table base_table_for_tenant_filter_test (tenant_id char(15) not null, " +
                "id char(5) not null, a_integer integer, a_string varchar(100) constraint pk primary key (tenant_id, id)) multi_tenant=true");
        createTestTable(getUrl(tenantId), "create view tenant_filter_test (tenant_col integer) AS SELECT * FROM BASE_TABLE_FOR_TENANT_FILTER_TEST");

        String query = "select * from tenant_filter_test where a_integer=0 and a_string='foo'";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(tenantId), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Filter filter = scan.getFilter();
        PTable table = plan.getTableRef().getTable();
        Expression aInteger = new ColumnRef(new TableRef(table), table.getColumnForColumnName("A_INTEGER").getPosition()).newColumnExpression();
        Expression aString = new ColumnRef(new TableRef(table), table.getColumnForColumnName("A_STRING").getPosition()).newColumnExpression();
        assertEquals(
            multiEncodedKVFilter(and(
                constantComparison(
                    CompareOperator.EQUAL,
                    aInteger,
                    0),
                constantComparison(
                    CompareOperator.EQUAL,
                    aString,
                    "foo")), TWO_BYTE_QUALIFIERS),
            filter);

        byte[] startRow = PVarchar.INSTANCE.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = startRow;
        assertArrayEquals(ByteUtil.nextKey(stopRow), scan.getStopRow());
    }

    @Test
    public void testScanCaching_Default() throws SQLException {
        String query = "select * from atable where a_integer=0";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        Configuration config = HBaseConfiguration.create();
        int defaultScannerCacheSize = config.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING,
                HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
        assertEquals(defaultScannerCacheSize, pstmt.getFetchSize());
        assertEquals(defaultScannerCacheSize, scan.getCaching());
    }

    @Test
    public void testScanCaching_CustomFetchSizeOnStatement() throws SQLException {
        String query = "select * from atable where a_integer=0";
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        final int FETCH_SIZE = 25;
        pstmt.setFetchSize(FETCH_SIZE);
        QueryPlan plan = pstmt.optimizeQuery();
        Scan scan = plan.getContext().getScan();
        assertEquals(FETCH_SIZE, pstmt.getFetchSize());
        assertEquals(FETCH_SIZE, scan.getCaching());
    }
    private Expression getDNF(PhoenixConnection pconn, String query) throws SQLException {
        //SQLParser parser = new SQLParser("where ID = 'i1' or (ID = 'i2' and A > 1)");
        //        ParseNode where = parser.parseWhereClause()
        PhoenixPreparedStatement pstmt = newPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.compileQuery();
        ParseNode where = plan.getStatement().getWhere();

        return transformDNF(where, plan.getContext());
    }
    @Test
    public void testWhereInclusion() throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(),
                PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        String ddl = "create table myTable(ID varchar primary key, A integer, B varchar, " +
                "C date, D double, E integer)";
        pconn.createStatement().execute(ddl);
        ddl = "create table myTableDesc(ID varchar primary key DESC, A integer, B varchar, " +
                "C date, D double, E integer)";
        pconn.createStatement().execute(ddl);

        final int NUM = 15;
        String[] containingQueries = new String[NUM];
        String[] containedQueries = new String[NUM];

        containingQueries[0] = "select * from myTable where ID = 'i1' or (ID = 'i2' and A > 1)";
        containedQueries[0] = "select * from myTableDesc where ID = 'i1' or (ID = 'i2' and " +
                "A > 2 + 2)";

        containingQueries[1] = "select * from myTable where ID > 'i3' and A > 1";
        containedQueries[1] = "select * from myTableDesc where (ID > 'i7' or ID = 'i4') and " +
                "A > 2 * 10";

        containingQueries[2] = "select * from myTable where ID IN ('i3', 'i7', 'i1') and A < 10";
        containedQueries[2] = "select * from myTableDesc where ID IN ('i1', 'i7') and A < 10 / 2";

        containingQueries[3] = "select * from myTableDesc where (ID, B) > ('i3', 'a') and A >= 10";
        containedQueries[3] = "select * from myTable where ID = 'i3' and B = 'c' and A = 10";

        containingQueries[4] = "select * from myTable where ID >= 'i3' and A between 5 and 15";
        containedQueries[4] = "select * from myTableDesc where ID = 'i3' and A between 5 and 10";

        containingQueries[5] = "select * from myTable where (A between 5 and 15) and " +
                "(D < 10.67 or C <= CURRENT_DATE())";
        containedQueries[5] = "select * from myTable where (A = 5 and D between 1.5 and 9.99) or " +
                "(A = 6 and C <= CURRENT_DATE() - 1000)";

        containingQueries[6] = "select * from myTable where A is not null";
        containedQueries[6] = "select * from myTable where A > 0";

        containingQueries[7] = "select * from myTable where NOT (B is null)";
        containedQueries[7] = "select * from myTable where (B > 'abc')";

        containingQueries[8] = "select * from myTable where A >= E and D <= A";
        containedQueries[8] = "select * from myTable where (A > E and D = A)";

        containingQueries[9] = "select * from myTable where A > E";
        containedQueries[9] = "select * from myTable where (A > E  and B is not null)";

        containingQueries[10] = "select * from myTable where B like '%abc'";
        containedQueries[10] = "select * from myTable where (B like '%abc' and ID > 'i1')";

        containingQueries[11] = "select * from myTable where " +
                "PHOENIX_ROW_TIMESTAMP() < CURRENT_TIME()";
        containedQueries[11] = "select * from myTable where " +
                "(PHOENIX_ROW_TIMESTAMP() < CURRENT_TIME() - 1)";

        containingQueries[12] = "select * from myTable where (A, E) IN ((2,3), (7,8), (10,11))";
        containedQueries[12] = "select * from myTable where (A, E) IN ((2,3), (7,8))";

        containingQueries[13] = "select * from myTable where ID > 'i3' and ID < 'i5'";
        containedQueries[13] = "select * from myTable where (ID = 'i4') ";

        containingQueries[14] = "select * from myTable where " +
                "CURRENT_DATE() - PHOENIX_ROW_TIMESTAMP() < 10";
        containedQueries[14] = "select * from myTable where " +
                " CURRENT_DATE() - PHOENIX_ROW_TIMESTAMP() < 5 ";

        for (int i = 0; i < NUM; i++) {
            Assert.assertTrue(WhereCompiler.contains(getDNF(pconn, containingQueries[i]),
                    getDNF(pconn, containedQueries[i])));
            Assert.assertFalse(WhereCompiler.contains(getDNF(pconn, containedQueries[i]),
                    getDNF(pconn, containingQueries[i])));
        }
    }

}
