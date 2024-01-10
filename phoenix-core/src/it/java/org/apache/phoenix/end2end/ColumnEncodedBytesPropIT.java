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

import static org.apache.phoenix.query.QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class ColumnEncodedBytesPropIT extends ParallelStatsDisabledIT {
	
	private String generateColsDDL(int numCols) {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<numCols; ++i) {
			if (i>0) {
				sb.append(" , ");
			}
			sb.append("col_").append(i).append(" VARCHAR ");
		}
		return sb.toString();
	}
	
	@Test
	public void testValidateProperty() throws SQLException {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName1 = SchemaUtil.getTableName("", generateUniqueName());
        String dataTableFullName2 = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            String ddl = "CREATE TABLE  " + dataTableFullName1 +
                    "  (id varchar not null, val varchar " + 
                    "  CONSTRAINT pk PRIMARY KEY (id)) COLUMN_ENCODED_BYTES=4";
            stmt.execute(ddl);
            
            ddl = "CREATE TABLE  " + dataTableFullName2 +
                    "  (id varchar not null, val varchar " + 
                    "  CONSTRAINT pk PRIMARY KEY (id)) COLUMN_ENCODED_BYTES=NONE";
            stmt.execute(ddl);
            
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            PTable dataTable1 = phxConn.getTable(new PTableKey(null, dataTableFullName1));
            assertEquals("Encoding scheme set incorrectly", QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS, dataTable1.getEncodingScheme());
            
            PTable dataTable2 = phxConn.getTable(new PTableKey(null, dataTableFullName2));
            assertEquals("Encoding scheme set incorrectly", QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, dataTable2.getEncodingScheme());
        } 
	}

	@Test
	public void testValidateMaxCols() throws SQLException {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            // create table with more cols than is supported by the encoding
            try {
                stmt.execute("CREATE TABLE  " + dataTableFullName +
                        "  (id varchar not null, " + generateColsDDL(QualifierEncodingScheme.ONE_BYTE_QUALIFIERS.getMaxQualifier()-QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE+2) + 
                        "  CONSTRAINT pk PRIMARY KEY (id)) COLUMN_ENCODED_BYTES=1");
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.MAX_COLUMNS_EXCEEDED.getErrorCode(), e.getErrorCode());
            }
            
            // create table with number of cols equal to that supported by the encoding
            stmt.execute("CREATE TABLE  " + dataTableFullName +
                    "  (id varchar not null, " + generateColsDDL(QualifierEncodingScheme.ONE_BYTE_QUALIFIERS.getMaxQualifier()-QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE+1) + 
                    "  CONSTRAINT pk PRIMARY KEY (id)) COLUMN_ENCODED_BYTES=1");
            
            // add one more column
            try {
                stmt.execute("ALTER TABLE  " + dataTableFullName + " ADD val_x VARCHAR");
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.MAX_COLUMNS_EXCEEDED.getErrorCode(), e.getErrorCode());
            }
        } 
	}
	
    @Test
    public void testAppendOnlySchema() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        String view1 = SchemaUtil.getTableName("", generateUniqueName());
        String view2 = SchemaUtil.getTableName("", generateUniqueName());
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE IMMUTABLE TABLE  " + dataTableFullName +
                    "  (id varchar not null, v1 varchar " + 
                    "  CONSTRAINT pk PRIMARY KEY (id)) COLUMN_ENCODED_BYTES=2, APPEND_ONLY_SCHEMA=true, UPDATE_CACHE_FREQUENCY=NEVER");
            stmt.execute("ALTER TABLE  " + dataTableFullName + "  ADD v2 varchar");
            
            stmt.execute("CREATE VIEW  " + view1 + "(v3 varchar, v4 varchar)" +
                    "  AS SELECT * FROM " + dataTableFullName + " WHERE v1='a'");
            stmt.execute("CREATE VIEW  " + view2 + "(v3 bigint, v4 integer)" +
                    "  AS SELECT * FROM " + dataTableFullName + " WHERE v1='b'");
            PTable v1 = conn.getTable(view1);
            PTable v2 = conn.getTable(view1);
            assertEquals(v1.getColumns().size(), v2.getColumns().size());
            for (int i = 1; i < v1.getColumns().size(); i++) {
                PColumn c1 = v1.getColumns().get(i);
                PColumn c2 = v2.getColumns().get(i);
                assertEquals(ENCODED_CQ_COUNTER_INITIAL_VALUE + i - Math.abs(Short.MIN_VALUE), Bytes.toShort(c1.getColumnQualifierBytes()));
                assertEquals(ENCODED_CQ_COUNTER_INITIAL_VALUE + i - Math.abs(Short.MIN_VALUE), Bytes.toShort(c2.getColumnQualifierBytes()));
            }
            
            // add one more column to confirm disallowed now
            try {
                stmt.execute("ALTER TABLE  " + dataTableFullName + "  ADD v5 varchar");
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            conn.setAutoCommit(true);
            stmt.execute("UPSERT INTO " + view1 + " VALUES('a','a','c','d','e')");
            stmt.execute("UPSERT INTO " + view2 + " VALUES('b','b','c',1, 2)");
            ResultSet rs1 = stmt.executeQuery("SELECT * FROM " + view1);
            assertTrue(rs1.next());
            assertEquals("a",rs1.getString(1));
            assertEquals("d",rs1.getString(4));
            assertEquals("e",rs1.getString(5));
            assertFalse(rs1.next());
            ResultSet rs2 = stmt.executeQuery("SELECT * FROM " + view2);
            assertTrue(rs2.next());
            assertEquals("b",rs2.getString(1));
            assertEquals(1L,rs2.getLong(4));
            assertEquals(2,rs2.getInt(5));
            assertFalse(rs2.next());
        } 
    }
}
