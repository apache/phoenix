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

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class SqlQueryToColumnInfoFunctionTest  extends BaseConnectionlessQueryTest {

    private Configuration configuration;
    private SqlQueryToColumnInfoFunction function;
    
    @Before
    public void setUp() throws SQLException {
        configuration = Mockito.mock(Configuration.class);
        Mockito.when(configuration.get(HConstants.ZOOKEEPER_QUORUM)).thenReturn(getUrl());
        function = new SqlQueryToColumnInfoFunction(configuration);
    }
    
    @Test
    public void testValidSelectQuery() throws SQLException {
        String ddl = "CREATE TABLE EMPLOYEE " +
                "  (id integer not null, name varchar, age integer,location varchar " +
                "  CONSTRAINT pk PRIMARY KEY (id))\n";
        createTestTable(getUrl(), ddl);
  
        final String selectQuery = "SELECT name as a ,age AS b,UPPER(location) AS c FROM EMPLOYEE";
        final ColumnInfo NAME_COLUMN = new ColumnInfo("A", Types.VARCHAR);
        final ColumnInfo AGE_COLUMN = new ColumnInfo("B", Types.INTEGER);
        final ColumnInfo LOCATION_COLUMN = new ColumnInfo("C", Types.VARCHAR);
        final List<ColumnInfo> expectedColumnInfos = ImmutableList.of(NAME_COLUMN, AGE_COLUMN,LOCATION_COLUMN);
        final List<ColumnInfo> actualColumnInfos = function.apply(selectQuery);
        Assert.assertEquals(expectedColumnInfos, actualColumnInfos);
        
    }
}
