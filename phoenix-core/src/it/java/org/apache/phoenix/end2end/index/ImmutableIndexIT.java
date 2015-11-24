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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class ImmutableIndexIT extends BaseHBaseManagedTimeIT {
	
	private final boolean localIndex;
	private final String tableDDLOptions;
	private final String tableName;
    private final String indexName;
    private final String fullTableName;
    private final String fullIndexName;
	
	public ImmutableIndexIT(boolean localIndex, boolean transactional) {
		this.localIndex = localIndex;
		StringBuilder optionBuilder = new StringBuilder("IMMUTABLE_ROWS=true");
		if (transactional) {
			optionBuilder.append(", TRANSACTIONAL=true");
		}
		this.tableDDLOptions = optionBuilder.toString();
		this.tableName = TestUtil.DEFAULT_DATA_TABLE_NAME + ( transactional ?  "_TXN" : "");
        this.indexName = "IDX" + ( transactional ?  "_TXN" : "");
        this.fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        this.fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
	}
	
	@Parameters(name="localIndex = {0} , transactional = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, false }, { false, true }, { true, false }, { true, true }
           });
    }
   
    @Test
    public void testDropIfImmutableKeyValueColumn() throws Exception {
    	Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
	        conn.setAutoCommit(false);
	        String ddl ="CREATE TABLE " + fullTableName + BaseTest.TEST_TABLE_SCHEMA + tableDDLOptions;
	        Statement stmt = conn.createStatement();
	        stmt.execute(ddl);
	        populateTestTable(fullTableName);
	        ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName + " (long_col1)";
	        stmt.execute(ddl);
	        
	        ResultSet rs;
	        
	        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
	        assertTrue(rs.next());
	        assertEquals(3,rs.getInt(1));
	        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
	        assertTrue(rs.next());
	        assertEquals(3,rs.getInt(1));
	        
	        conn.setAutoCommit(true);
	        String dml = "DELETE from " + fullTableName + " WHERE long_col2 = 4";
	        try {
	            conn.createStatement().execute(dml);
	            fail();
	        } catch (SQLException e) {
	            assertEquals(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS.getErrorCode(), e.getErrorCode());
	        }
	            
	        conn.createStatement().execute("DROP TABLE " + fullTableName);
        }
    }
    

}
