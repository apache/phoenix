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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

import com.google.common.collect.Lists;

public class PhoenixEncodeDecodeTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testDecodeValues1() throws Exception {
        testDecodeValues(false, false);
    }
    
    @Test
    public void testDecodeValues2() throws Exception {
        testDecodeValues(true, false);
    }
    
    @Test
    public void testDecodeValues3() throws Exception {
        testDecodeValues(true, true);
    }
    
    @Test
    public void testDecodeValues4() throws Exception {
        testDecodeValues(false, true);
    }
    
    @SuppressWarnings("unchecked")
    private void testDecodeValues(boolean nullFixedWidth, boolean nullVariableWidth) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE T(pk1 CHAR(15) not null, pk2 VARCHAR not null, CF1.v1 DATE, CF2.v2 VARCHAR, CF2.v1 VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (pk1, pk2)) ");
        
        Date d = nullFixedWidth ? null : new Date(100);
        String s = nullVariableWidth ? null : "foo";
        Object[] values = new Object[] {"def", "eid", d, s, s};
        byte[] bytes = PhoenixRuntime.encodeValues(conn, "T", values, Lists.newArrayList(new Pair<String, String>(null, "pk1"), new Pair<String, String>(null, "pk2"), new Pair<String, String>("cf1", "v1"), new Pair<String, String>("cf2", "v2"), new Pair<String, String>("cf2", "v1")));
        Object[] decodedValues = PhoenixRuntime.decodeValues(conn, "T", bytes, Lists.newArrayList(new Pair<String, String>(null, "pk1"), new Pair<String, String>(null, "pk2"), new Pair<String, String>("cf1", "v1"), new Pair<String, String>("cf2", "v2"), new Pair<String, String>("cf2", "v1")));
        assertEquals(Lists.newArrayList("def", "eid", d, s, s), Arrays.asList(decodedValues));
    }
    
}
