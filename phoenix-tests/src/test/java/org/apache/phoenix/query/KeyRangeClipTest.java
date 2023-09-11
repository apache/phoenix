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
package org.apache.phoenix.query;

import static org.apache.phoenix.query.KeyRange.UNBOUND;
import static org.apache.phoenix.query.QueryConstants.DESC_SEPARATOR_BYTE_ARRAY;
import static org.apache.phoenix.query.QueryConstants.SEPARATOR_BYTE_ARRAY;
import static org.junit.Assert.assertEquals;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.iterate.BaseResultIterators;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


/**
 * Test for intersect method in {@link SkipScanFilter}
 */
@RunWith(Parameterized.class)
public class KeyRangeClipTest extends BaseConnectionlessQueryTest {
    private final RowKeySchema schema;
    private final KeyRange input;
    private final KeyRange expectedOutput;
    private final int clipTo;

    private static byte[] getRange(PhoenixConnection pconn, List<Object> startValues) throws SQLException {
        byte[] lowerRange;
        if (startValues == null) {
            lowerRange = KeyRange.UNBOUND;
        } else {
            String upsertValues = StringUtils.repeat("?,", startValues.size()).substring(0,startValues.size() * 2 - 1);
            String upsertStmt = "UPSERT INTO T VALUES(" + upsertValues + ")";
            PreparedStatement stmt = pconn.prepareStatement(upsertStmt);
            for (int i = 0; i < startValues.size(); i++) {
                stmt.setObject(i+1, startValues.get(i));
            }
            stmt.execute();
            Cell startCell = PhoenixRuntime.getUncommittedDataIterator(pconn).next().getSecond().get(0);
            lowerRange = CellUtil.cloneRow(startCell);
            pconn.rollback();
        }
        return lowerRange;
    }
    
    public KeyRangeClipTest(String tableDef, List<Object> startValues, List<Object> endValues, int clipTo, KeyRange expectedOutput) throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("CREATE TABLE T(" + tableDef+ ")");
        PTable table = pconn.getMetaDataCache().getTableRef(new PTableKey(null,"T")).getTable();
        this.schema = table.getRowKeySchema();
        byte[] lowerRange = getRange(pconn, startValues);
        byte[] upperRange = getRange(pconn, endValues);
        this.input = KeyRange.getKeyRange(lowerRange, upperRange);
        this.expectedOutput = expectedOutput;
        this.clipTo = clipTo;
    }

    @After
    public void cleanup() throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
        pconn.createStatement().execute("DROP TABLE T");
    }
    
    @Test
    public void test() {
        ScanRanges scanRanges = ScanRanges.create(schema, Collections.<List<KeyRange>>singletonList(Collections.<KeyRange>singletonList(input)), new int[] {schema.getFieldCount()-1}, null, false, -1);
        ScanRanges clippedRange = BaseResultIterators.computePrefixScanRanges(scanRanges, clipTo);
        assertEquals(expectedOutput, clippedRange.getScanRange());
    }

    @Parameters(name="KeyRangeClipTest_{0}")
    public static synchronized Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(Lists.newArrayList( // [XY - *]
                "A VARCHAR NOT NULL, B VARCHAR, C VARCHAR, CONSTRAINT PK PRIMARY KEY (A,B,C)",
                Lists.newArrayList("XY",null,"Z"), null, 2,
                KeyRange.getKeyRange(Bytes.toBytes("XY"), true, UNBOUND, false)).toArray());
        testCases.add(Lists.newArrayList(
                "A VARCHAR NOT NULL, B VARCHAR, C VARCHAR, CONSTRAINT PK PRIMARY KEY (A,B,C)",
                null, Lists.newArrayList("XY",null,"Z"), 2,
                KeyRange.getKeyRange(
                        ByteUtil.nextKey(SEPARATOR_BYTE_ARRAY), true, // skips null values for unbound lower
                        ByteUtil.nextKey(ByteUtil.concat(Bytes.toBytes("XY"),SEPARATOR_BYTE_ARRAY,SEPARATOR_BYTE_ARRAY,SEPARATOR_BYTE_ARRAY)), false)).toArray());
        testCases.add(Lists.newArrayList(
                "A VARCHAR NOT NULL, B VARCHAR, C VARCHAR, D VARCHAR, CONSTRAINT PK PRIMARY KEY (A,B,C,D)",
                Lists.newArrayList("XY",null,null,"Z"), null, 3,
                KeyRange.getKeyRange(Bytes.toBytes("XY"), true, UNBOUND, false)).toArray());
        testCases.add(Lists.newArrayList(
                "A VARCHAR NOT NULL, B VARCHAR, C VARCHAR, D VARCHAR, CONSTRAINT PK PRIMARY KEY (A,B,C,D)",
                null, Lists.newArrayList("XY",null,null,"Z"), 3,
                KeyRange.getKeyRange(
                        ByteUtil.nextKey(SEPARATOR_BYTE_ARRAY), true, // skips null values for unbound lower
                        ByteUtil.nextKey(ByteUtil.concat(Bytes.toBytes("XY"),SEPARATOR_BYTE_ARRAY,SEPARATOR_BYTE_ARRAY,SEPARATOR_BYTE_ARRAY,SEPARATOR_BYTE_ARRAY)), false)).toArray());
        testCases.add(Lists.newArrayList(
                "A CHAR(1) NOT NULL, B CHAR(1) NOT NULL, C CHAR(1) NOT NULL, CONSTRAINT PK PRIMARY KEY (A,B,C)",
                Lists.newArrayList("A","B","C"), Lists.newArrayList("C","D","E"), 2,
                KeyRange.getKeyRange(Bytes.toBytes("AB"), true, ByteUtil.nextKey(Bytes.toBytes("CD")), false)).toArray());
        testCases.add(Lists.newArrayList(
                "A VARCHAR NOT NULL, B VARCHAR, C SMALLINT NOT NULL, D VARCHAR, CONSTRAINT PK PRIMARY KEY (A,B,C,D)",
                Lists.<Object>newArrayList("XY",null,1,"Z"), null, 3,
                KeyRange.getKeyRange(ByteUtil.concat(Bytes.toBytes("XY"), SEPARATOR_BYTE_ARRAY, SEPARATOR_BYTE_ARRAY, PSmallint.INSTANCE.toBytes(1)), true, UNBOUND, false)).toArray());
        testCases.add(Lists.newArrayList(
                "A VARCHAR NOT NULL, B BIGINT NOT NULL, C VARCHAR, CONSTRAINT PK PRIMARY KEY (A,B DESC,C)",
                Lists.<Object>newArrayList("XYZ",1,"Z"), null, 2,
                KeyRange.getKeyRange(ByteUtil.concat(Bytes.toBytes("XYZ"), SEPARATOR_BYTE_ARRAY, PLong.INSTANCE.toBytes(1, SortOrder.DESC)), true, UNBOUND, false)).toArray());
        testCases.add(Lists.newArrayList(
                "A VARCHAR NOT NULL, B VARCHAR, C VARCHAR, CONSTRAINT PK PRIMARY KEY (A DESC,B,C)",
                null, Lists.newArrayList("XY",null,"Z"), 3,
                KeyRange.getKeyRange(
                        ByteUtil.nextKey(SEPARATOR_BYTE_ARRAY), true, // skips null values for unbound lower
                        (ByteUtil.concat(PVarchar.INSTANCE.toBytes("XY",SortOrder.DESC),DESC_SEPARATOR_BYTE_ARRAY,SEPARATOR_BYTE_ARRAY,Bytes.toBytes("Z"))), false)).toArray());
        return testCases;
    }
}
