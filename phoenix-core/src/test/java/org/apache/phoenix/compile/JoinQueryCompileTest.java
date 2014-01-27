/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.junit.Test;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.QueryUtil;

/**
 * Test compilation of queries containing joins.
 */
public class JoinQueryCompileTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testExplainPlan() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String query = "EXPLAIN SELECT s.supplier_id, order_id, c.name, i.name, quantity, o.date FROM " + JOIN_ORDER_TABLE + " o LEFT JOIN " 
    	+ JOIN_CUSTOMER_TABLE + " c ON o.customer_id = c.customer_id AND c.name LIKE 'C%' LEFT JOIN " 
    	+ JOIN_ITEM_TABLE + " i ON o.item_id = i.item_id RIGHT JOIN " 
    	+ JOIN_SUPPLIER_TABLE + " s ON s.supplier_id = i.supplier_id WHERE i.name LIKE 'T%'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertEquals(
        		"CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_SUPPLIER_TABLE\n" +
        		"    SERVER FILTER BY FIRST KEY ONLY\n" +
        		"    PARALLEL EQUI-JOIN 1 HASH TABLES:\n" +
        		"    BUILD HASH TABLE 0\n" +
        		"        CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ORDER_TABLE\n" +
        		"            PARALLEL EQUI-JOIN 2 HASH TABLES:\n" +
        		"            BUILD HASH TABLE 0\n" +
        		"                CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_CUSTOMER_TABLE\n" +
        		"                    SERVER FILTER BY NAME LIKE 'C%'\n" +
        		"            BUILD HASH TABLE 1\n" +
        		"                CLIENT PARALLEL 1-WAY FULL SCAN OVER JOIN_ITEM_TABLE\n" +
        		"    AFTER-JOIN SERVER FILTER BY I.NAME LIKE 'T%'", QueryUtil.getExplainPlan(rs));
    }

}
