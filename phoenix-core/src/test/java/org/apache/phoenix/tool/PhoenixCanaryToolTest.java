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
package org.apache.phoenix.tool;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class PhoenixCanaryToolTest {

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private PreparedStatement ps;

    @Mock
    private ResultSet rs;

    @Mock
    private DatabaseMetaData dbm;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void upsertTableTest() throws Exception {
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(Mockito.anyString())).thenReturn(ps);
        when(statement.executeUpdate(Mockito.anyString())).thenReturn(1);
        CanaryTestResult result = new PhoenixCanaryTool.UpsertTableTest().runTest(connection);
        assertEquals(true, result.isSuccessful());
        assertEquals("Test upsertTable successful", result.getMessage());
    }

    @Test
    public void readTableTest() throws Exception {
        when(connection.prepareStatement(Mockito.anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getInt(1)).thenReturn(1);
        when(rs.getString(2)).thenReturn("Hello World");
        CanaryTestResult result = new PhoenixCanaryTool.ReadTableTest().runTest(connection);
        assertEquals(true, result.isSuccessful());
        assertEquals("Test readTable successful", result.getMessage());
    }

    @Test
    public void failTest() throws Exception {
        when(connection.prepareStatement(Mockito.anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.getInt(1)).thenReturn(3);
        when(rs.getString(2)).thenReturn("Incorrect data");
        when(rs.next()).thenReturn(true).thenReturn(false);
        CanaryTestResult result = new PhoenixCanaryTool.ReadTableTest().runTest(connection);
        assertEquals(false, result.isSuccessful());
        assert (result.getMessage().contains("Retrieved values do not match the inserted values"));
    }
}