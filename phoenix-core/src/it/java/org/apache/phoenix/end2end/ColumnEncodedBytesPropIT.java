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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

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
	
}
