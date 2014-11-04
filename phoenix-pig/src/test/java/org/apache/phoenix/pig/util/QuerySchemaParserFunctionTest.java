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

import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

/**
 * 
 * Unit tests to validate the query passed to LOAD .
 *
 */
public class QuerySchemaParserFunctionTest extends BaseConnectionlessQueryTest {

    private Configuration configuration;
    private QuerySchemaParserFunction function;
    
    @Before
    public void setUp() throws SQLException {
        configuration = Mockito.mock(Configuration.class);
        Mockito.when(configuration.get(HConstants.ZOOKEEPER_QUORUM)).thenReturn(getUrl());
        function = new QuerySchemaParserFunction(configuration);
    }
    
    @Test(expected=RuntimeException.class)
    public void testSelectQuery() {
        final String selectQuery = "SELECT col1 FROM test";
        function.apply(selectQuery);
        fail("Should fail as the table [test] doesn't exist");
   }
    
    @Test
    public void testValidSelectQuery() throws SQLException {
        String ddl = "CREATE TABLE EMPLOYEE " +
                "  (id integer not null, name varchar, age integer,location varchar " +
                "  CONSTRAINT pk PRIMARY KEY (id))\n";
        createTestTable(getUrl(), ddl);
  
        final String selectQuery = "SELECT name,age,location FROM EMPLOYEE";
        Pair<String,String> pair = function.apply(selectQuery);
         
        assertEquals(pair.getFirst(), "EMPLOYEE");
        assertEquals(pair.getSecond(),Joiner.on(',').join("NAME","AGE","LOCATION"));
    }
    
    @Test(expected=RuntimeException.class)
    public void testUpsertQuery() throws SQLException {
        String ddl = "CREATE TABLE EMPLOYEE " +
                "  (id integer not null, name varchar, age integer,location varchar " +
                "  CONSTRAINT pk PRIMARY KEY (id))\n";
        createTestTable(getUrl(), ddl);
  
        final String upsertQuery = "UPSERT INTO EMPLOYEE (ID, NAME) VALUES (?, ?)";
        
        function.apply(upsertQuery);
        fail(" Function call successful despite passing an UPSERT query");
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testAggregationQuery() throws SQLException {
        String ddl = "CREATE TABLE EMPLOYEE " +
                "  (id integer not null, name varchar, age integer,location varchar " +
                "  CONSTRAINT pk PRIMARY KEY (id))\n";
        createTestTable(getUrl(), ddl);
  
        final String selectQuery = "SELECT MAX(ID) FROM EMPLOYEE";
        function.apply(selectQuery);
        fail(" Function call successful despite passing an aggreagate query");
    }
}
