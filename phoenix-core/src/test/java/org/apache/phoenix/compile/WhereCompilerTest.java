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

import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.and;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.apache.phoenix.util.TestUtil.bindParams;
import static org.apache.phoenix.util.TestUtil.columnComparison;
import static org.apache.phoenix.util.TestUtil.constantComparison;
import static org.apache.phoenix.util.TestUtil.in;
import static org.apache.phoenix.util.TestUtil.multiKVFilter;
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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.NumberUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;


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
                CompareOp.EQUAL,
                A_INTEGER,
                0)),
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
        pconn.createStatement().execute("CREATE TABLE t (k varchar primary key, v varchar) SALT_BUCKETS=20");
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
        byte[] expectedEndKey = ByteUtil.nextKey(ByteUtil.concat(key, QueryConstants.SEPARATOR_BYTE_ARRAY));
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
            multiKVFilter(columnComparison(
                CompareOp.EQUAL,
                A_STRING,
                B_STRING)),
            filter);
    }

    @Test
    public void testCollapseFunctionToNull() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,null) = 'foo'";
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
            multiKVFilter(and(
                constantComparison(
                    CompareOp.EQUAL,
                    A_INTEGER,
                    0),
                constantComparison(
                    CompareOp.EQUAL,
                    A_STRING,
                    "foo"))),
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
                CompareOp.LESS_OR_EQUAL,
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
                CompareOp.GREATER_OR_EQUAL,
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
                CompareOp.GREATER_OR_EQUAL,
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
                constantComparison(CompareOp.EQUAL,
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
                    CompareOp.EQUAL,
                    new SubstrFunction(Arrays.<Expression> asList(
                        new RowKeyColumnExpression(
                            ENTITY_ID,
                            new RowKeyValueAccessor(ATABLE.getPKColumns(), 1)),
                        LiteralExpression.newConstant(1),
                        LiteralExpression.newConstant(3))),
                    keyPrefix),
                constantComparison(
                    CompareOp.EQUAL,
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
                CompareOp.EQUAL,
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
                CompareOp.EQUAL,
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
                plan.getTableRef().getTable().getRowKeySchema()),
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
                plan.getTableRef().getTable().getRowKeySchema()),
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
                SchemaUtil.VAR_BINARY_SCHEMA),
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
                        true))),
                plan.getTableRef().getTable().getRowKeySchema()),
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
                SchemaUtil.VAR_BINARY_SCHEMA),
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
                SchemaUtil.VAR_BINARY_SCHEMA),
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
                        CompareOp.GREATER_OR_EQUAL,
                        A_INTEGER,
                        0),
                    constantComparison(
                        CompareOp.LESS_OR_EQUAL,
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
                        CompareOp.GREATER_OR_EQUAL,
                        A_INTEGER,
                        0),
                    constantComparison(
                        CompareOp.LESS_OR_EQUAL,
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

        assertEquals(
            multiKVFilter(and(
                constantComparison(
                    CompareOp.EQUAL,
                    A_INTEGER,
                    0),
                constantComparison(
                    CompareOp.EQUAL,
                    A_STRING,
                    "foo"))),
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

        assertEquals(
            multiKVFilter(and(
                constantComparison(
                    CompareOp.EQUAL,
                    A_INTEGER,
                    0),
                constantComparison(
                    CompareOp.EQUAL,
                    A_STRING,
                    "foo"))),
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
        assertEquals(QueryServicesOptions.DEFAULT_SCAN_CACHE_SIZE, pstmt.getFetchSize());
        assertEquals(QueryServicesOptions.DEFAULT_SCAN_CACHE_SIZE, scan.getCaching());
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
}
