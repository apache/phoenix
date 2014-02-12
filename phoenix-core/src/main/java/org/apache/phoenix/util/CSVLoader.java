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
package org.apache.phoenix.util;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.Maps;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PDataType;

import java.io.FileReader;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * Upserts CSV data using Phoenix JDBC connection
 * 
 * 
 * 
 */
public class CSVLoader {

	private final PhoenixConnection conn;
	private final String tableName;
    private final List<String> columns;
    private final boolean isStrict;
    private final List<String> delimiter;
    private final Map<String,Character> ctrlTable = new HashMap<String,Character>() {
        {   put("1",'\u0001');
            put("2",'\u0002');
            put("3",'\u0003');
            put("4",'\u0004');
            put("5",'\u0005');
            put("6",'\u0006');
            put("7",'\u0007');
            put("8",'\u0008');
            put("9",'\u0009');}};
    
    private int unfoundColumnCount;

    public CSVLoader(PhoenixConnection conn, String tableName, List<String> columns, boolean isStrict,List<String> delimiter) {
        this.conn = conn;
        this.tableName = tableName;
        this.columns = columns;
        this.isStrict = isStrict;
        this.delimiter = delimiter;
    }

	public CSVLoader(PhoenixConnection conn, String tableName, List<String> columns, boolean isStrict) {
       this(conn,tableName,columns,isStrict,null);
    }


    /**
	 * Upserts data from CSV file. Data is batched up based on connection batch
	 * size. Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. Note: Column Names are
	 * expected as first line of CSV file.
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public void upsert(String fileName) throws Exception {
        List<String> delimiter = this.delimiter;
        CSVReader reader;
        if ((delimiter != null) && (delimiter.size() == 3)) {
            reader = new CSVReader(new FileReader(fileName),
                getCSVCustomField(this.delimiter.get(0)),
                getCSVCustomField(this.delimiter.get(1)),
                getCSVCustomField(this.delimiter.get(2)));
        } else {
            reader = new CSVReader(new FileReader(fileName));
        }
        upsert(reader);
	}


    public char getCSVCustomField(String field) {
        if(this.ctrlTable.containsKey(field)) {
            return this.ctrlTable.get(field);
        } else {
            return field.charAt(0);
        }
    }

	/**
	 * Upserts data from CSV file. Data is batched up based on connection batch
	 * size. Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. Note: Column Names are
	 * expected as first line of CSV file.
	 * 
	 * @param reader CSVReader instance
	 * @throws Exception
	 */
	public void upsert(CSVReader reader) throws Exception {
	    List<String> columns = this.columns;
	    if (columns != null && columns.isEmpty()) {
	        columns = Arrays.asList(reader.readNext());
	    }
		ColumnInfo[] columnInfo = generateColumnInfo(columns);
        PreparedStatement stmt = null;
        PreparedStatement[] stmtCache = null;
		if (columns == null) {
		    stmtCache = new PreparedStatement[columnInfo.length];
		} else {
		    String upsertStatement = QueryUtil.constructUpsertStatement(columnInfo, tableName, columnInfo.length - unfoundColumnCount);
		    stmt = conn.prepareStatement(upsertStatement);
		}
		String[] nextLine;
		int rowCount = 0;
		int upsertBatchSize = conn.getMutateBatchSize();
		boolean wasAutoCommit = conn.getAutoCommit();
		try {
    		conn.setAutoCommit(false);
    		Object upsertValue = null;
    		long start = System.currentTimeMillis();
    
    		// Upsert data based on SqlType of each column
    		while ((nextLine = reader.readNext()) != null) {
    		    if (columns == null) {
    		        stmt = stmtCache[nextLine.length-1];
    		        if (stmt == null) {
    	                String upsertStatement = QueryUtil.constructUpsertStatement(columnInfo, tableName, nextLine.length);
    	                stmt = conn.prepareStatement(upsertStatement);
    	                stmtCache[nextLine.length-1] = stmt;
    		        }
    		    }
    			for (int index = 0; index < columnInfo.length; index++) {
    			    if (columnInfo[index] == null) {
    			        continue;
    			    }
                    String line = nextLine[index];
                    Integer info = columnInfo[index].getSqlType();
                    upsertValue = convertTypeSpecificValue(line, info);
    				if (upsertValue != null) {
    					stmt.setObject(index + 1, upsertValue, columnInfo[index].getSqlType());
    				} else {
    					stmt.setNull(index + 1, columnInfo[index].getSqlType());
    				}
    			}
    			stmt.execute();
    
    			// Commit when batch size is reached
    			if (++rowCount % upsertBatchSize == 0) {
    				conn.commit();
    				System.out.println("Rows upserted: " + rowCount);
    			}
    		}
    		conn.commit();
    		double elapsedDuration = ((System.currentTimeMillis() - start) / 1000.0);
    		System.out.println("CSV Upsert complete. " + rowCount + " rows upserted");
    		System.out.println("Time: " + elapsedDuration + " sec(s)\n");
		} finally {
		    if(stmt != null) {
		        stmt.close();
		    }
		    if (wasAutoCommit) conn.setAutoCommit(true);
		}
	}
	
	/**
	 * Gets CSV string input converted to correct type 
	 */
	private Object convertTypeSpecificValue(String s, Integer sqlType) throws Exception {
	    return PDataType.fromTypeId(sqlType).toObject(s);
	}

	/**
	 * Get array of ColumnInfos that contain Column Name and its associated
	 * PDataType
	 * 
	 * @param columns
	 * @return
	 * @throws SQLException
	 */
	private ColumnInfo[] generateColumnInfo(List<String> columns)
			throws SQLException {
	    Map<String,Integer> columnNameToTypeMap = Maps.newLinkedHashMap();
        DatabaseMetaData dbmd = conn.getMetaData();
        // TODO: escape wildcard characters here because we don't want that behavior here
        String escapedTableName = StringUtil.escapeLike(tableName);
        String[] schemaAndTable = escapedTableName.split("\\.");
        ResultSet rs = null;
        try {
            rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? "" : schemaAndTable[0]),
                    (schemaAndTable.length == 1 ? escapedTableName : schemaAndTable[1]),
                    null);
            while (rs.next()) {
                columnNameToTypeMap.put(rs.getString(QueryUtil.COLUMN_NAME_POSITION), rs.getInt(QueryUtil.DATA_TYPE_POSITION));
            }
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        ColumnInfo[] columnType;
	    if (columns == null) {
            int i = 0;
            columnType = new ColumnInfo[columnNameToTypeMap.size()];
            for (Map.Entry<String, Integer> entry : columnNameToTypeMap.entrySet()) {
                columnType[i++] = new ColumnInfo('"' + entry.getKey() + '"',entry.getValue());
            }
	    } else {
            // Leave "null" as indication to skip b/c it doesn't exist
            columnType = new ColumnInfo[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                String columnName = SchemaUtil.normalizeIdentifier(columns.get(i).trim());
                Integer sqlType = columnNameToTypeMap.get(columnName);
                if (sqlType == null) {
                    if (isStrict) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                            .setColumnName(columnName).setTableName(tableName).build().buildException();
                    }
                    unfoundColumnCount++;
                } else {
                    columnType[i] = new ColumnInfo('"' + columnName + '"', sqlType);
                }
            }
            if (unfoundColumnCount == columns.size()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                    .setColumnName(Arrays.toString(columns.toArray(new String[0]))).setTableName(tableName).build().buildException();
            }
	    }
		return columnType;
	}
}
