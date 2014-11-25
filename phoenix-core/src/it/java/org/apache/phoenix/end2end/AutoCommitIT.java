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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class AutoCommitIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testMutationJoin() throws Exception {
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        
        String ddl = "CREATE TABLE test_table " +
                "  (row varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (row))\n";
        createTestTable(getUrl(), ddl);
        
        String query = "UPSERT INTO test_table(row, col1) VALUES('row1', 1)";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();
        
        conn.setAutoCommit(false);
        query = "UPSERT INTO test_table(row, col1) VALUES('row1', 2)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        
        query = "DELETE FROM test_table WHERE row='row1'";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();
        
        query = "SELECT * FROM test_table";
        statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        assertFalse(rs.next());

        query = "DELETE FROM test_table WHERE row='row1'";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();

        query = "UPSERT INTO test_table(row, col1) VALUES('row1', 3)";
        statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();
        
        query = "SELECT * FROM test_table";
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        assertTrue(rs.next());
        assertEquals("row1", rs.getString(1));
        assertEquals(3, rs.getInt(2));

        conn.close();
    }
}
