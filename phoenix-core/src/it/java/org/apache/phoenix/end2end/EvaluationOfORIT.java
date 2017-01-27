/**
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
package org.apache.phoenix.end2end;
 
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class EvaluationOfORIT extends ParallelStatsDisabledIT{

    @Test
    public void testFalseOrFalse() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("SELECT (FALSE OR FALSE) AS B FROM \"SYSTEM\".\"CATALOG\" LIMIT 1");
        assertTrue(rs.next());
        assertFalse(rs.getBoolean(1));
        conn.close();
    }
    
	@Test
	public void testPKOrNotPKInOREvaluation() throws SQLException {
	    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
	    Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
	    conn.setAutoCommit(false);
	    
            String create = "CREATE TABLE " + tableName + " ( ID INTEGER NOT NULL PRIMARY KEY,NAME VARCHAR(50))";
            conn.createStatement().execute(create);
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                     tableName + " VALUES (?, ?)");

            stmt.setInt(1, 1);
            stmt.setString(2, "Tester1");
            stmt.execute();
            
            stmt.setInt(1,2);
            stmt.setString(2, "Tester2");
            stmt.execute();
            
            stmt.setInt(1,3);
            stmt.setString(2, "Tester3");
            stmt.execute();
            
            stmt.setInt(1,4);
            stmt.setString(2, "LikeTester1");
            stmt.execute();
            
            stmt.setInt(1,5);
            stmt.setString(2, "LikeTester2");
            stmt.execute();
            
            stmt.setInt(1,6);
            stmt.setString(2, "LikeTesterEnd");
            stmt.execute();
            
            stmt.setInt(1,7);
            stmt.setString(2, "LikeTesterEnd2");
            stmt.execute();
            
            stmt.setInt(1,8);
            stmt.setString(2, "Tester3");
            stmt.execute();
            
            stmt.setInt(1,9);
            stmt.setString(2, "Tester4");
            stmt.execute();
            
            stmt.setInt(1,10);
            stmt.setString(2, "Tester5");
            stmt.execute();
            
            stmt.setInt(1,11);
            stmt.setString(2, "Tester6");
            stmt.execute();
            
            stmt.setInt(1,12);
            stmt.setString(2, "tester6");
            stmt.execute();
            
            stmt.setInt(1,13);
            stmt.setString(2, "lester1");
            stmt.execute();
            
            stmt.setInt(1,14);
            stmt.setString(2, "le50ster1");
            stmt.execute();
            
            stmt.setInt(1,15);
            stmt.setString(2, "LE50ster1");
            stmt.execute();
            
            stmt.setInt(1,16);
            stmt.setString(2, "LiketesterEnd");
            stmt.execute();
            
            stmt.setInt(1,17);
            stmt.setString(2, "la50ster1");
            stmt.execute();
            
            stmt.setInt(1,18);
            stmt.setString(2, "lA50ster0");
            stmt.execute();
            
            stmt.setInt(1,19);
            stmt.setString(2, "lA50ster2");
            stmt.execute();
            
            stmt.setInt(1,20);
            stmt.setString(2, "la50ster0");
            stmt.execute();
            
            stmt.setInt(1,21);
            stmt.setString(2, "la50ster2");
            stmt.execute();
            
            stmt.setInt(1,22);
            stmt.setString(2, "La50ster3");
            stmt.execute();
            
            stmt.setInt(1,23);
            stmt.setString(2, "la50ster3");
            stmt.execute();
            
            stmt.setInt(1,24);
            stmt.setString(2, "l[50ster3");
            stmt.execute();
            
            stmt.setInt(1,25);
            stmt.setString(2, "Tester1");
            stmt.execute();
            
            stmt.setInt(1,26);
            stmt.setString(2, "Tester100");
            stmt.execute();		   
            conn.commit();
            
            String select = "Select * from " + tableName + " where ID=6 or Name between 'Tester1' and 'Tester3'";
            ResultSet rs;
            rs = conn.createStatement().executeQuery(select);
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(6,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(8,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(25,rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(26,rs.getInt(1));		        
            conn.close();               
	}
    
	 
}
