/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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

import java.util.List;

import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests methods on {@link ColumnInfoToStringEncoderDecoder}
 */
public class ColumnInfoToStringEncoderDecoderTest {

    @Test
    public void testEncode() {
        final ColumnInfo columnInfo = new ColumnInfo("col1", PVarchar.INSTANCE.getSqlType());
        final String encodedColumnInfo = ColumnInfoToStringEncoderDecoder.encode(Lists.newArrayList(columnInfo));
        assertEquals(columnInfo.toString(),encodedColumnInfo);
    }
    
    @Test
    public void testDecode() {
        final ColumnInfo columnInfo = new ColumnInfo("col1", PVarchar.INSTANCE.getSqlType());
        final String encodedColumnInfo = ColumnInfoToStringEncoderDecoder.encode(Lists.newArrayList(columnInfo));
        assertEquals(columnInfo.toString(),encodedColumnInfo);
    }
    
    @Test
    public void testEncodeDecodeWithNulls() {
        final ColumnInfo columnInfo1 = new ColumnInfo("col1", PVarchar.INSTANCE.getSqlType());
        final ColumnInfo columnInfo2 = null;
        final String columnInfoStr = ColumnInfoToStringEncoderDecoder.encode(Lists.newArrayList(columnInfo1,columnInfo2));
        final List<ColumnInfo> decodedColumnInfo = ColumnInfoToStringEncoderDecoder.decode(columnInfoStr);
        assertEquals(1,decodedColumnInfo.size()); 
    }

}
