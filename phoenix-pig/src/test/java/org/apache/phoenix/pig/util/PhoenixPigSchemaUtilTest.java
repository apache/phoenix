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
package org.apache.phoenix.pig.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.ColumnInfoToStringEncoderDecoder;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * 
 * Tests on PhoenixPigSchemaUtil
 */
public class PhoenixPigSchemaUtilTest {

    private static final ColumnInfo ID_COLUMN = new ColumnInfo("ID", Types.BIGINT);
    private static final ColumnInfo NAME_COLUMN = new ColumnInfo("NAME", Types.VARCHAR);
    private static final ColumnInfo LOCATION_COLUMN = new ColumnInfo("LOCATION", Types.ARRAY);
    
    
    @Test
    public void testSchema() throws SQLException, IOException {
        
        final Configuration configuration = mock(Configuration.class);
        final List<ColumnInfo> columnInfos = ImmutableList.of(ID_COLUMN,NAME_COLUMN);
        final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnInfos);
        when(configuration.get(PhoenixConfigurationUtil.SELECT_COLUMN_INFO_KEY)).thenReturn(encodedColumnInfos);
        when(configuration.get(PhoenixConfigurationUtil.SCHEMA_TYPE)).thenReturn(SchemaType.TABLE.name());
        final ResourceSchema actual = PhoenixPigSchemaUtil.getResourceSchema(configuration);
        
        // expected schema.
        final ResourceFieldSchema[] fields = new ResourceFieldSchema[2];
        fields[0] = new ResourceFieldSchema().setName("ID")
                                                .setType(DataType.LONG);

        fields[1] = new ResourceFieldSchema().setName("NAME")
                                                .setType(DataType.CHARARRAY);
        final ResourceSchema expected = new ResourceSchema().setFields(fields);
        
        assertEquals(expected.toString(), actual.toString());
        
    }
    
    @Test(expected=IllegalDataException.class)
    public void testUnSupportedTypes() throws SQLException, IOException {
        
        final Configuration configuration = mock(Configuration.class);
        final List<ColumnInfo> columnInfos = ImmutableList.of(ID_COLUMN,LOCATION_COLUMN);
        final String encodedColumnInfos = ColumnInfoToStringEncoderDecoder.encode(columnInfos);
        when(configuration.get(PhoenixConfigurationUtil.SELECT_COLUMN_INFO_KEY)).thenReturn(encodedColumnInfos);
        PhoenixPigSchemaUtil.getResourceSchema(configuration);
        fail("We currently don't support Array type yet. WIP!!");
    }
}
