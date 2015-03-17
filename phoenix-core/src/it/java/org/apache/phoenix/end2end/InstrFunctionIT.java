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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;

public class InstrFunctionIT extends BaseHBaseManagedTimeIT{
    
    private void initTable(Connection conn, String sortOrder, String s) throws Exception {
        String ddl = "CREATE TABLE INSTR_FUNC_TEST (pk VARCHAR NOT NULL PRIMARY KEY " + sortOrder + ", kv VARCHAR)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO INSTR_FUNC_TEST VALUES(?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, s);
        stmt.execute();
        conn.commit();        
    }
    
    private void testInstr(Connection conn, String s) throws Exception {
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT instr(pk,'eni') FROM INSTR_FUNC_TEST");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
    }
    
    
    @Test
    public void testInstrFunction() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String s = "Phoenix Rising";
        initTable(conn, "ASC", s);
        testInstr(conn, s);
    }

}
