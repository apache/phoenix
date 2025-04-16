/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.*;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.util.*;
import org.junit.Test;

/**
 * Test for compiling the various cursor related statements
 * @since 0.1
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED",
    justification = "Test code.")
public class CursorCompilerTest extends BaseConnectionlessQueryTest {

  @Test
  public void testCursorLifecycleCompile() throws SQLException {
    String query = "SELECT a_string, b_string FROM atable";
    String sql = "DECLARE testCursor CURSOR FOR " + query;
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    // this verifies PHOENIX-5534 is fixed and we don't initialize metrics twice
    // on a cursor query
    props.put("phoenix.query.request.metrics.enabled", "true");

    Connection conn = DriverManager.getConnection(getUrl(), props);
    // Test declare cursor compile
    PreparedStatement statement = conn.prepareStatement(sql);
    // Test declare cursor execution
    statement.execute();
    assertTrue(CursorUtil.cursorDeclared("testCursor"));
    // Test open cursor compile
    sql = "OPEN testCursor";
    statement = conn.prepareStatement(sql);
    // Test open cursor execution
    statement.execute();
    // Test fetch cursor compile
    sql = "FETCH NEXT FROM testCursor";
    statement = conn.prepareStatement(sql);
    statement.executeQuery();
  }
}
