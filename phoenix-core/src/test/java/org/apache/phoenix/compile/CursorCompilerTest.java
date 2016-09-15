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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.CountAggregator;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.filter.ColumnProjectionFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.util.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.assertDegenerate;
import static org.junit.Assert.*;


/**
 * 
 * Test for compiling the various cursor related statements
 *
 * 
 * @since 0.1
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
        value="RV_RETURN_VALUE_IGNORED",
        justification="Test code.")
public class CursorCompilerTest extends BaseConnectionlessQueryTest {

    @Test
    public void testCursorLifecycleCompile() throws SQLException {
        String query = "SELECT a_string, b_string FROM atable";
        String sql = "DECLARE testCursor CURSOR FOR " + query;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        //Test declare cursor compile
        PreparedStatement statement = conn.prepareStatement(sql);
        //Test declare cursor execution
        statement.execute();
        assertTrue(CursorUtil.cursorDeclared("testCursor"));
        //Test open cursor compile
        sql = "OPEN testCursor";
        statement = conn.prepareStatement(sql);
        //Test open cursor execution
        statement.execute();
        //Test fetch cursor compile
        sql = "FETCH NEXT FROM testCursor";
        statement = conn.prepareStatement(sql);
        statement.executeQuery();
    }
}