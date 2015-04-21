/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.workload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.apache.phoenix.pherf.result.file.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.util.PhoenixUtil;

import difflib.DiffUtils;
import difflib.Patch;

public class QueryVerifier {
	private PhoenixUtil pUtil = new PhoenixUtil();
	private static final Logger logger = LoggerFactory
			.getLogger(QueryVerifier.class);
	private boolean useTemporaryOutput;
	private String directoryLocation;

	public QueryVerifier(boolean useTemporaryOutput) {
		this.useTemporaryOutput = useTemporaryOutput;
		this.directoryLocation = this.useTemporaryOutput ? 
				PherfConstants.EXPORT_TMP : PherfConstants.EXPORT_DIR;
		
		ensureBaseDirExists();
	}
	
	/***
	 * Export query resultSet to CSV file
	 * @param query
	 * @throws Exception
	 */
	public String exportCSV(Query query) throws Exception {
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		String fileName = getFileName(query);
		FileOutputStream fos = new FileOutputStream(fileName);
		try {
			conn = pUtil.getConnection(query.getTenantId());
			statement = conn.prepareStatement(query.getStatement());
			boolean isQuery = statement.execute();
			if (isQuery) {
				rs = statement.executeQuery();
				int columnCount = rs.getMetaData().getColumnCount();
				while (rs.next()) {
					for (int columnNum = 1; columnNum <= columnCount; columnNum++) {
						fos.write((rs.getString(columnNum) + PherfConstants.RESULT_FILE_DELIMETER).getBytes());
					}
					fos.write(PherfConstants.NEW_LINE.getBytes());
				}
			} else {
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) rs.close();
			if (statement != null) statement.close();
			if (conn != null) conn.close();
			fos.flush();
			fos.close();
		}
		return fileName;
	}
	
	/***
	 * Do a diff between exported query results and temporary CSV file
	 * @param query
	 * @param newCSV
	 * @return
	 */
	public boolean doDiff(Query query, String newCSV) {
        List<String> original = fileToLines(getCSVName(query, PherfConstants.EXPORT_DIR, ""));
        List<String> newLines  = fileToLines(newCSV);
        
        Patch patch = DiffUtils.diff(original, newLines);
        if (patch.getDeltas().isEmpty()) {
        	logger.info("Match: " + query.getId() + " with " + newCSV);
        	return true;
        } else {
        	logger.error("DIFF FAILED: " + query.getId() + " with " + newCSV);
        	return false;
        }
	}
	
	/***
	 * Helper method to load file
	 * @param filename
	 * @return
	 */
    private static List<String> fileToLines(String filename) {
            List<String> lines = new LinkedList<String>();
            String line = "";
            try {
                    BufferedReader in = new BufferedReader(new FileReader(filename));
                    while ((line = in.readLine()) != null) {
                            lines.add(line);
                    }
                    in.close();
            } catch (IOException e) {
                    e.printStackTrace();
            }
            
            return lines;
    }

    /**
     * Get explain plan for a query
     * @param query
     * @return
     * @throws SQLException
     */
	public String getExplainPlan(Query query) throws SQLException {
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement statement = null;
		StringBuilder buf = new StringBuilder();
		try {
			conn = pUtil.getConnection(query.getTenantId());
			statement = conn.prepareStatement("EXPLAIN " + query.getStatement());
			rs = statement.executeQuery();
	        while (rs.next()) {
	            buf.append(rs.getString(1).trim().replace(",", "-"));
	        }
			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) rs.close();
			if (statement != null) statement.close();
			if (conn != null) conn.close();
		}
		return buf.toString();
	}
	
    /***
     * Helper method to generate CSV file name
     * @param query
     * @return
     * @throws FileNotFoundException
     */
	private String getFileName(Query query) throws FileNotFoundException {
		String tempExt = "";
		if (this.useTemporaryOutput) {
			tempExt = "_" + java.util.UUID.randomUUID().toString();
		}
		return getCSVName(query, this.directoryLocation, tempExt);
	}
	
	private String getCSVName(Query query, String directory, String tempExt) {
		String csvFile = directory + PherfConstants.PATH_SEPARATOR
		        + query.getId() + tempExt + Extension.CSV.toString();
				return csvFile;
	}
	
	private void ensureBaseDirExists() {
		File baseDir = new File(this.directoryLocation);
		if (!baseDir.exists()) {
			baseDir.mkdir();
		}
	}
}
