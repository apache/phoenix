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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.apache.phoenix.util.TestUtil.assertEmptyScanKey;
import static org.apache.phoenix.util.TestUtil.like;
import static org.apache.phoenix.util.TestUtil.not;
import static org.apache.phoenix.util.TestUtil.rowKeyFilter;
import static org.apache.phoenix.util.TestUtil.substr;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.filter.RowKeyComparisonFilter;
import org.apache.phoenix.filter.SingleKeyValueComparisonFilter;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;



public class WhereOptimizerTest extends BaseConnectionlessQueryTest {
    
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
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        TestUtil.bindParams(pstmt, binds);
        QueryPlan plan = pstmt.compileQuery();
        assertEquals(limit, plan.getLimit());
        return plan.getContext();
    }

    @Test
    public void testSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testReverseSingleKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where '" + tenantId + "' = organization_id";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }

    @Test
    public void testStartKeyStopKey() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key)");
        conn.close();

        String query = "select * from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes("EA"), scan.getStartRow());
        assertArrayEquals(PDataType.VARCHAR.toBytes("EZ"), scan.getStopRow());
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)), 15));
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(startDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
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
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryGreaterThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testGreaterThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')>?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLessThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryLessThanRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-01 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testLessThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 01:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();
        assertNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBoundaryLessThanOrEqualRound() throws Exception {
        String inst = "a";
        String host = "b";
        Date startDate = DateUtil.parseDate("2012-01-01 00:00:00");
        Date endDate = DateUtil.parseDate("2012-01-02 00:00:00");
        String query = "select * from ptsdb where inst=? and host=? and round(date,'DAY')<=?";
        List<Object> binds = Arrays.<Object>asList(inst,host,startDate);
        Scan scan = compileStatement(query, binds).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(inst),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.VARCHAR.toBytes(host),QueryConstants.SEPARATOR_BYTE_ARRAY,
                PDataType.DATE.toBytes(endDate));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testOverlappingKeyExpression() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String entityId = "002333333333333";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3)='" + keyPrefix + "' and entity_id='" + entityId + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(startRow, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
    }

    @Test
    public void testTrailingSubstrExpression() throws SQLException {
        String tenantId = "0xD000000000001";
        String entityId = "002333333333333";
        String query = "select * from atable where substr(organization_id,1,3)='" + tenantId.substring(0, 3) + "' and entity_id='" + entityId + "'";
        Scan scan = compileStatement(query).getScan();
        assertNotNull(scan.getFilter());

        byte[] startRow = ByteUtil.concat(ByteUtil.fillKey(PDataType.VARCHAR.toBytes(tenantId.substring(0,3)),15),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        // Even though the first slot is a non inclusive range, we need to do a next key
        // on the second slot because of the algorithm we use to seek to and terminate the
        // loop during skip scan. We could end up having a first slot just under the upper
        // limit of slot one and a value equal to the value in slot two and we need this to
        // be less than the upper range that would get formed.
        byte[] stopRow = ByteUtil.concat(ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId.substring(0,3))),15),ByteUtil.nextKey(PDataType.VARCHAR.toBytes(entityId)));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testBasicRangeExpression() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id <= '" + tenantId + "'";
        Scan scan = compileStatement(query).getScan();
        assertNull(scan.getFilter());

        assertTrue(scan.getStartRow().length == 0);
        byte[] stopRow = ByteUtil.concat(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)));
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

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix1),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix2),15));
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

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix1),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix2)),15));
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix1)),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix2)),15));
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(ByteUtil.concat(stopRow, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
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

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(keyPrefix1),new byte[entityId.length() - keyPrefix1.length()]);
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),PDataType.VARCHAR.toBytes(entityId));
        assertArrayEquals(ByteUtil.concat(stopRow, QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
    }

    @Test
    public void testKeyRangeExpression9() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix1 = "002";
        String keyPrefix2 = "0033";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(entity_id,1,3) >= '" + keyPrefix1 + "' and substr(entity_id,1,4) <= '" + keyPrefix2 + "'";
        Scan scan = compileStatement(query).getScan();

        assertNull(scan.getFilter());
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix1),15)); // extra byte is due to implicit internal padding
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix2)),15));
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
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
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
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }

    @Test
    public void testRHSLiteral() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and 0 >= a_integer limit 1000";
        StatementContext context = compileStatement(query, 1000);
        Scan scan = context.getScan();

        assertNotNull(scan.getFilter());
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
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
        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
        assertArrayEquals(stopRow, scan.getStopRow());
    }

    @Test
    public void testDegenerateLikeNoWildcard() throws SQLException {
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select * from atable where organization_id LIKE ? and entity_id  LIKE '" + keyPrefix + "'";
        List<Object> binds = Arrays.<Object>asList(tenantId);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        assertDegenerate(scan);
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
                    likeArg)),
                filter);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
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
                    likeArg)),
                filter);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        assertArrayEquals(startRow, scan.getStartRow());
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
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
                    likeArg)),
                filter);

        byte[] startRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15));
        byte[] stopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId),ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15));
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
                    likeArg)),
                filter);

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
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
                    likeArg)),
                filter);

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
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
                    likeArg))),
                filter);

        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId);
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
        ensureTableCreated(getUrl(),"PKIntValueTest");
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
        ensureTableCreated(getUrl(), "PKUnsignedIntValueTest");
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
        ensureTableCreated(getUrl(), "PKUnsignedLongValueTest");
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
                PDataType.CHAR.getKeyRange(PDataType.CHAR.toBytes(tenantId1), true, PDataType.CHAR.toBytes(tenantId1), true), 
                PDataType.CHAR.getKeyRange(PDataType.CHAR.toBytes(tenantId2), true, PDataType.CHAR.toBytes(tenantId2), true)));
        assertEquals(expectedRanges, ranges);
        byte[] startRow = PDataType.VARCHAR.toBytes(tenantId1);
        assertArrayEquals(startRow, scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId2)), scan.getStopRow());
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
                PDataType.CHAR.getKeyRange(
                        ByteUtil.fillKey(PDataType.CHAR.toBytes("00D"),15), true, 
                        ByteUtil.fillKey(ByteUtil.nextKey(PDataType.CHAR.toBytes("00D")),15), false), 
                PDataType.CHAR.getKeyRange(
                        ByteUtil.fillKey(PDataType.CHAR.toBytes("foo"),15), true, 
                        ByteUtil.fillKey(ByteUtil.nextKey(PDataType.CHAR.toBytes("foo")),15), false)));
        assertEquals(expectedRanges, ranges);
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
        assertEquals(Long.valueOf(5399179882L), PDataType.UNSIGNED_LONG.toObject(scanRanges.getRanges().get(2).get(0).getLowerRange()));
    }
    
    @Test
    public void testForceRangeScanKeepsFilters() throws SQLException {
        ensureTableCreated(getUrl(), TestUtil.ENTITY_HISTORY_TABLE_NAME);
        String tenantId = "000000000000001";
        String keyPrefix = "002";
        String query = "select /*+ RANGE_SCAN */ ORGANIZATION_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID from " + TestUtil.ENTITY_HISTORY_TABLE_NAME + 
                " where ORGANIZATION_ID=? and SUBSTR(PARENT_ID, 1, 3) = ? and  CREATED_DATE >= ? and CREATED_DATE < ? order by ORGANIZATION_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID limit 6";
        Date startTime = new Date(System.currentTimeMillis());
        Date stopTime = new Date(startTime.getTime() + TestUtil.MILLIS_IN_DAY);
        List<Object> binds = Arrays.<Object>asList(tenantId, keyPrefix, startTime, stopTime);
        StatementContext context = compileStatement(query, binds, 6);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof RowKeyComparisonFilter);

        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), ByteUtil.fillKey(PDataType.VARCHAR.toBytes(keyPrefix),15), PDataType.DATE.toBytes(startTime));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        byte[] expectedStopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), ByteUtil.fillKey(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(keyPrefix)),15), PDataType.DATE.toBytes(stopTime));
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.CHAR.toBytes(tenantId), PDataType.CHAR.toBytes(entityId));
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
    }

    
    @Test
    public void testRVCExpressionThroughOr() throws SQLException {
        String tenantId = "000000000000001";
        String entityId = "002333333333331";
        String entityId1 = "002333333333330";
        String entityId2 = "002333333333332";
        String query = "select * from atable where (organization_id,entity_id) >= (?,?) and organization_id = ? and  (entity_id = ? or entity_id = ?)";
        List<Object> binds = Arrays.<Object>asList(tenantId, entityId, tenantId, entityId1, entityId2);
        StatementContext context = compileStatement(query, binds);
        Scan scan = context.getScan();
        Filter filter = scan.getFilter();
        assertNotNull(filter);
        assertTrue(filter instanceof SkipScanFilter);
        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), PDataType.VARCHAR.toBytes(entityId));
        byte[] expectedStopRow = ByteUtil.concat(ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), PDataType.VARCHAR.toBytes(entityId2)), QueryConstants.SEPARATOR_BYTE_ARRAY);
        assertArrayEquals(expectedStartRow, scan.getStartRow());
        assertArrayEquals(expectedStopRow, scan.getStopRow());
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), PDataType.VARCHAR.toBytes(parentId));
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(lowerTenantId), PDataType.VARCHAR.toBytes(lowerParentId), PDataType.DATE.toBytes(lowerCreatedDate));
        byte[] expectedStopRow = ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(upperTenantId), PDataType.VARCHAR.toBytes(upperParentId)));
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(lowerTenantId), PDataType.VARCHAR.toBytes(lowerParentId), PDataType.DATE.toBytes(lowerCreatedDate));
        byte[] expectedStopRow = ByteUtil.nextKey(ByteUtil.concat(ByteUtil.fillKey(PDataType.VARCHAR.toBytes("7"),15), ByteUtil.fillKey(PDataType.VARCHAR.toBytes("7"), 15)));
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
        byte[] expectedStartRow = PDataType.VARCHAR.toBytes(subStringTenantId);
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), PDataType.VARCHAR.toBytes(subStringParentId));
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
        byte[] expectedStopRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), ByteUtil.nextKey(PDataType.VARCHAR.toBytes(subStringParentId)));
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.VARCHAR.toBytes(tenantId), PDataType.VARCHAR.toBytes(parentId));
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
        byte[] expectedStartRow = ByteUtil.concat(PDataType.CHAR.toBytes(tenantId), new byte[15], ByteUtil.previousKey(PDataType.DATE.toBytes(createdDate)));
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
        byte[] expectedStartRow = ByteUtil.concat(new byte[15], ByteUtil.previousKey(PDataType.CHAR.toBytes(parentId)), PDataType.DATE.toBytes(createdDate));
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
        assertArrayEquals(ByteUtil.concat(PDataType.VARCHAR.toBytes(firstOrgId), PDataType.VARCHAR.toBytes(parentId), PDataType.DATE.toBytes(createdDate)), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(secondOrgId)), scan.getStopRow());
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
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStartRow());
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
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStartRow());
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
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStartRow());
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
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
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
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
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
        assertArrayEquals(ByteUtil.concat(PDataType.VARCHAR.toBytes(firstTenantId), PDataType.VARCHAR.toBytes(firstParentId)), scan.getStartRow());
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
        assertArrayEquals(ByteUtil.concat(PDataType.VARCHAR.toBytes(firstTenantId), PDataType.VARCHAR.toBytes(firstParentId)), scan.getStartRow());
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
        assertTrue(filter instanceof RowKeyComparisonFilter);
        assertArrayEquals(HConstants.EMPTY_START_ROW, scan.getStartRow());
        assertArrayEquals(HConstants.EMPTY_END_ROW, scan.getStopRow());
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
        assertTrue(filter instanceof RowKeyComparisonFilter);
        assertArrayEquals(ByteUtil.concat(PDataType.VARCHAR.toBytes(firstOrgId), PDataType.VARCHAR.toBytes(firstParentId)), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(ByteUtil.concat(PDataType.VARCHAR.toBytes(secondOrgId), PDataType.VARCHAR.toBytes(secondParentId))), scan.getStopRow());
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
                KeyRange.getKeyRange(ByteUtil.concat(PDataType.CHAR.toBytes(firstOrgId), PDataType.CHAR.toBytes(firstParentId))), 
                KeyRange.getKeyRange(ByteUtil.concat(PDataType.CHAR.toBytes(secondOrgId), PDataType.CHAR.toBytes(secondParentId)))));
        assertEquals(skipScanRanges, context.getScanRanges().getRanges());
        assertArrayEquals(ByteUtil.concat(PDataType.CHAR.toBytes(firstOrgId), PDataType.CHAR.toBytes(firstParentId)), scan.getStartRow());
        assertArrayEquals(ByteUtil.concat(PDataType.CHAR.toBytes(secondOrgId), PDataType.CHAR.toBytes(secondParentId), QueryConstants.SEPARATOR_BYTE_ARRAY), scan.getStopRow());
    }
}
