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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.stats.StatisticsUtil;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

public class SystemSplitPolicyTest {
    @Test
    public void testStatsSplitPolicy() {
        SplitOnLeadingVarCharColumnsPolicy policy = new SystemStatsSplitPolicy();
        byte[] splitOn;
        byte[] rowKey;
        byte[] table;
        ImmutableBytesWritable family;
        table = PVarchar.INSTANCE.toBytes("FOO.BAR");
        family = QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES_PTR;
        rowKey = ByteUtil.concat(
                PLong.INSTANCE.toBytes(20L), 
                PVarchar.INSTANCE.toBytes("BAS"), 
                QueryConstants.SEPARATOR_BYTE_ARRAY, 
                PInteger.INSTANCE.toBytes(100));
        splitOn = StatisticsUtil.getRowKey(table, family, rowKey);
        splitOn = policy.getSplitPoint(splitOn);
        assertArrayEquals(ByteUtil.concat(table, QueryConstants.SEPARATOR_BYTE_ARRAY), splitOn);
        
        table = PVarchar.INSTANCE.toBytes("MY_TABLE");
        family = new ImmutableBytesWritable(Bytes.toBytes("ABC"));
        rowKey = ByteUtil.concat(
                PVarchar.INSTANCE.toBytes("BAS"), 
                QueryConstants.SEPARATOR_BYTE_ARRAY, 
                PInteger.INSTANCE.toBytes(100),
                PLong.INSTANCE.toBytes(20L));
        splitOn = StatisticsUtil.getRowKey(table, family, rowKey);
        splitOn = policy.getSplitPoint(splitOn);
        assertArrayEquals(ByteUtil.concat(table, QueryConstants.SEPARATOR_BYTE_ARRAY), splitOn);
    }
    
    private static byte[] getSystemFunctionRowKey(String tenantId, String funcName, String typeName, byte[] argPos) {
        return ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), 
                QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(funcName),
                QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(typeName), 
                QueryConstants.SEPARATOR_BYTE_ARRAY,
                argPos
                );
    }
    
    private static byte[] getSystemFunctionSplitKey(String tenantId, String funcName) {
        return ByteUtil.concat(PVarchar.INSTANCE.toBytes(tenantId), 
                QueryConstants.SEPARATOR_BYTE_ARRAY,
                PVarchar.INSTANCE.toBytes(funcName),
                QueryConstants.SEPARATOR_BYTE_ARRAY);
    }
    
    @Test
    public void testFunctionSplitPolicy() {
        SplitOnLeadingVarCharColumnsPolicy policy = new SystemFunctionSplitPolicy();
        byte[] splitPoint;
        byte[] rowKey;
        byte[] expectedSplitPoint;
        rowKey = getSystemFunctionRowKey("","MY_FUNC", "VARCHAR", Bytes.toBytes(3));
        expectedSplitPoint = getSystemFunctionSplitKey("","MY_FUNC");
        splitPoint = policy.getSplitPoint(rowKey);
        assertArrayEquals(expectedSplitPoint, splitPoint);
        
        rowKey = getSystemFunctionRowKey("TENANT1","F", "", Bytes.toBytes(3));
        expectedSplitPoint = getSystemFunctionSplitKey("TENANT1","F");
        splitPoint = policy.getSplitPoint(rowKey);
        assertArrayEquals(expectedSplitPoint, splitPoint);
    }
}
