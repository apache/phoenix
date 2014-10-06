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

import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_ORDER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_DISPLAY_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.phoenix.compile.JoinCompiler.JoinTable;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

/**
 * Test compilation of queries containing joins.
 */
public class JoinQueryCompilerTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testExplainPlan() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String query = "EXPLAIN SELECT s.\"supplier_id\", \"order_id\", c.name, i.name, quantity, o.date FROM " + JOIN_ORDER_TABLE_FULL_NAME + " o LEFT JOIN " 
    	+ JOIN_CUSTOMER_TABLE_FULL_NAME + " c ON o.\"customer_id\" = c.\"customer_id\" AND c.name LIKE 'C%' LEFT JOIN " 
    	+ JOIN_ITEM_TABLE_FULL_NAME + " i ON o.\"item_id\" = i.\"item_id\" RIGHT JOIN " 
    	+ JOIN_SUPPLIER_TABLE_FULL_NAME + " s ON s.\"supplier_id\" = i.\"supplier_id\" WHERE i.name LIKE 'T%'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertEquals(
        		"CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_DISPLAY_NAME + "\n" +
        		"    SERVER FILTER BY FIRST KEY ONLY\n" +
        		"    PARALLEL LEFT-JOIN TABLE 0\n" +
        		"        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_DISPLAY_NAME + "\n" +
        		"            PARALLEL LEFT-JOIN TABLE 0\n" +
        		"                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_CUSTOMER_TABLE_DISPLAY_NAME + "\n" +
        		"                    SERVER FILTER BY NAME LIKE 'C%'\n" +
        		"            PARALLEL LEFT-JOIN TABLE 1\n" +
        		"                CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_DISPLAY_NAME + "\n" +
        		"    AFTER-JOIN SERVER FILTER BY I.NAME LIKE 'T%'", QueryUtil.getExplainPlan(rs));
    }

    @Test
    public void testWhereClauseOptimization() throws Exception {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        String queryTemplate = "SELECT t1.\"item_id\", t2.\"item_id\", t3.\"item_id\" FROM " + JOIN_ITEM_TABLE_FULL_NAME + " t1 " 
                + "%s JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " t2 ON t1.\"item_id\" = t2.\"item_id\" " 
                + "%s JOIN " + JOIN_ITEM_TABLE_FULL_NAME + " t3 ON t1.\"item_id\" = t3.\"item_id\" " 
                + "WHERE t1.\"item_id\" = '0000000001' AND t2.\"item_id\" = '0000000002' AND t3.\"item_id\" = '0000000003'";

        String query = String.format(queryTemplate, "INNER", "INNER");
        JoinTable joinTable = getJoinTable(query, pconn);
        assertEquals(1, joinTable.getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "INNER", "LEFT");
        joinTable = getJoinTable(query, pconn);
        assertEquals(1, joinTable.getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "INNER", "RIGHT");
        joinTable = getJoinTable(query, pconn);
        assertEquals(0, joinTable.getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "LEFT", "INNER");
        joinTable = getJoinTable(query, pconn);
        assertEquals(1, joinTable.getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "LEFT", "LEFT");
        joinTable = getJoinTable(query, pconn);
        assertEquals(1, joinTable.getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "LEFT", "RIGHT");
        joinTable = getJoinTable(query, pconn);
        assertEquals(0, joinTable.getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "RIGHT", "INNER");
        joinTable = getJoinTable(query, pconn);
        assertEquals(0, joinTable.getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());

        query = String.format(queryTemplate, "RIGHT", "RIGHT");
        joinTable = getJoinTable(query, pconn);
        assertEquals(0, joinTable.getTable().getPreFilters().size());
        assertEquals(0, joinTable.getJoinSpecs().get(0).getJoinTable().getTable().getPreFilters().size());
        assertEquals(1, joinTable.getJoinSpecs().get(1).getJoinTable().getTable().getPreFilters().size());
    }
    
    private static JoinTable getJoinTable(String query, PhoenixConnection connection) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement select = SubselectRewriter.flatten(parser.parseQuery(), connection);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, connection);
        select = StatementNormalizer.normalize(select, resolver);
        SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolver, connection);
        if (transformedSelect != select) {
            resolver = FromCompiler.getResolverForQuery(transformedSelect, connection);
            select = StatementNormalizer.normalize(transformedSelect, resolver);
        }
        PhoenixStatement stmt = connection.createStatement().unwrap(PhoenixStatement.class);
        return JoinCompiler.compile(stmt, select, resolver);        
    }
}

