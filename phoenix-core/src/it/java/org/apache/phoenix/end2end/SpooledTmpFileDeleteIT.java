/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;



public class SpooledTmpFileDeleteIT extends BaseHBaseManagedTimeIT {
	private Connection conn = null;
	private Properties props = null;
	private File spoolDir;

	@Before 
	public void setup() throws SQLException {
		props = new Properties();
		spoolDir =  Files.createTempDir();
		props.put(QueryServices.SPOOL_DIRECTORY, spoolDir.getPath());
        props.setProperty(QueryServices.SPOOL_THRESHOLD_BYTES_ATTRIB, Integer.toString(1));
		conn = DriverManager.getConnection(getUrl(), props);
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE test (ID varchar NOT NULL PRIMARY KEY) SPLIT ON ('EA','EZ')");
		stmt.execute("UPSERT INTO test VALUES ('AA')");
		stmt.execute("UPSERT INTO test VALUES ('EB')");    
		stmt.execute("UPSERT INTO test VALUES ('FA')");    
		stmt.close();
		conn.commit();
	}
	
	@After
	public void tearDown() throws Exception {
	    if (spoolDir != null) {
	        spoolDir.delete();
	    }
	}

	@Test
	public void testDeleteAllSpooledTmpFiles() throws SQLException, Throwable {
		File dir = new File(spoolDir.getPath());
		File[] files = null; 

		class FilenameFilter implements FileFilter {
			@Override
			public boolean accept(File dir) {
				return dir.getName().toLowerCase().endsWith(".bin") && 
						dir.getName().startsWith("ResultSpooler");
			}
		}

		FilenameFilter fnameFilter = new FilenameFilter();

		// clean up first
		files = dir.listFiles(fnameFilter);
		for (File file : files) {
			file.delete();
		}

		String query = "select * from TEST";
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		assertTrue(rs.next());
		files = dir.listFiles(fnameFilter);
		assertTrue(files.length > 0);
		List<String> fileNames = new ArrayList<String>();
		for (File file : files) {
			fileNames.add(file.getName());
		}

		String preparedQuery = "select * from test where id = ?";
		PreparedStatement pstmt = conn.prepareStatement(preparedQuery);
		pstmt.setString(1, "EB");
		ResultSet prs = pstmt.executeQuery(preparedQuery);
		assertTrue(prs.next());
		files = dir.listFiles(fnameFilter);
		assertTrue(files.length > 0);
		for (File file : files) {
			fileNames.add(file.getName());
		}

		Connection conn2 = DriverManager.getConnection(getUrl(), props);
		String query2 = "select * from TEST";
		Statement statement2 = conn2.createStatement();
		ResultSet rs2 = statement2.executeQuery(query2);
		assertTrue(rs2.next());
		files = dir.listFiles(fnameFilter);
		assertTrue(files.length > 0);

		String preparedQuery2 = "select * from test where id = ?";
		PreparedStatement pstmt2 = conn2.prepareStatement(preparedQuery2);
		pstmt2.setString(1, "EB");
		ResultSet prs2 = pstmt2.executeQuery(preparedQuery2);
		assertTrue(prs2.next());
		files = dir.listFiles(fnameFilter);
		assertTrue(files.length > 0);

		conn.close();

		files = dir.listFiles(fnameFilter);

		for (File file : files) {
			assertFalse(fileNames.contains(file.getName()));
		}
		conn2.close();
		files = dir.listFiles(fnameFilter);
		assertTrue(files.length == 0);
	}

}
