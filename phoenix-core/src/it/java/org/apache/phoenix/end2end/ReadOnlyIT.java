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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class ReadOnlyIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testConnectionReadOnly() throws Exception {
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE test_table " +
                        "  (row varchar not null, col1 integer" +
                        "  CONSTRAINT pk PRIMARY KEY (row))\n"; 
        createTestTable(getUrl(), ddl);

        String query = "UPSERT INTO test_table(row, col1) VALUES('row1', 777)";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();

	try{    
		conn.setReadOnly(true);
                assertTrue(conn.isReadOnly());
		ddl = "CREATE TABLE test_table2 " +
				"  (row varchar not null, col1 integer" +
				"  CONSTRAINT pk PRIMARY KEY (row))\n";
		statement = conn.prepareStatement(ddl);
        	statement.executeUpdate();
        	conn.commit();
		fail();
	} catch (SQLException e) {
              assertTrue(e.getMessage(), e.getMessage().contains("ERROR 518 (25502): Mutations are not permitted for a read-only connection."));
        }
	  
	try {  
                query = "UPSERT INTO test_table(row, col1) VALUES('row1', 888)";
                statement = conn.prepareStatement(query);
                statement.executeUpdate();
                conn.commit();
                fail();
        } catch (SQLException e) {
              assertTrue(e.getMessage(), e.getMessage().contains("ERROR 518 (25502): Mutations are not permitted for a read-only connection."));
        }

	conn.setReadOnly(false);
        assertFalse(conn.isReadOnly());
        ddl = "ALTER TABLE test_table ADD col2 VARCHAR";
	statement = conn.prepareStatement(ddl);
        statement.executeUpdate();
	conn.commit();

        try {   
		conn.setReadOnly(true);
                ddl = "ALTER TABLE test_table ADD col3 VARCHAR";
		statement = conn.prepareStatement(ddl);
        	statement.executeUpdate();
                fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 518 (25502): Mutations are not permitted for a read-only connection."));
        }

  }
}
