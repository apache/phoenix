/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests methods on {@link ColumnInfoToStringEncoderDecoder}
 */
public class ColumnInfoToStringEncoderDecoderTest {

    @Test
    public void testEncodeDecode() {
    	final Configuration configuration = new Configuration ();
        final ColumnInfo columnInfo1 = new ColumnInfo("col1", PVarchar.INSTANCE.getSqlType());
        final ColumnInfo columnInfo2 = new ColumnInfo("col2", PDate.INSTANCE.getSqlType());
        ArrayList<ColumnInfo> expectedColInfos = Lists.newArrayList(columnInfo1,columnInfo2);
		ColumnInfoToStringEncoderDecoder.encode(configuration, expectedColInfos);
        
		//verify the configuration has the correct values
        assertEquals(2, configuration.getInt(ColumnInfoToStringEncoderDecoder.CONFIGURATION_COUNT, 0));
        assertEquals(columnInfo1.toString(), configuration.get(String.format("%s_%d", ColumnInfoToStringEncoderDecoder.CONFIGURATION_VALUE_PREFIX, 0)));
        assertEquals(columnInfo2.toString(), configuration.get(String.format("%s_%d", ColumnInfoToStringEncoderDecoder.CONFIGURATION_VALUE_PREFIX, 1)));
        
        List<ColumnInfo> actualColInfos = ColumnInfoToStringEncoderDecoder.decode(configuration);
        assertEquals(expectedColInfos, actualColInfos);
    }
    
    @Test
    public void testEncodeDecodeWithNulls() {
    	final Configuration configuration = new Configuration ();
        final ColumnInfo columnInfo1 = new ColumnInfo("col1", PVarchar.INSTANCE.getSqlType());
        ArrayList<ColumnInfo> expectedColInfos = Lists.newArrayList(columnInfo1);
		ColumnInfoToStringEncoderDecoder.encode(configuration, Lists.newArrayList(columnInfo1, null));
        
		//verify the configuration has the correct values
        assertEquals(1, configuration.getInt(ColumnInfoToStringEncoderDecoder.CONFIGURATION_COUNT, 0));
        assertEquals(columnInfo1.toString(), configuration.get(String.format("%s_%d", ColumnInfoToStringEncoderDecoder.CONFIGURATION_VALUE_PREFIX, 0)));
        
        List<ColumnInfo> actualColInfos = ColumnInfoToStringEncoderDecoder.decode(configuration);
        assertEquals(expectedColInfos, actualColInfos);
    }

}
