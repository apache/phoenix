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

import java.io.File;
import java.io.Reader;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PDataType;

import com.google.common.collect.Maps;

/***
 * Upserts CSV data using Phoenix JDBC connection
 * 
 * 
 * 
 */
public class CSVCommonsLoader {

	private final PhoenixConnection conn;
	private final String tableName;
	private final List<String> columns;
	private final boolean isStrict;
    boolean userSuppliedMetaCharacters = false;
	private final List<String> customMetaCharacters;
	private PhoenixHeaderSource headerSource = PhoenixHeaderSource.FROM_TABLE;
	private final CSVFormat format;
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
	
    public enum PhoenixHeaderSource {
    	FROM_TABLE,
    	IN_LINE,
    	SUPPLIED_BY_USER
    }

	public CSVCommonsLoader(PhoenixConnection conn, String tableName,
			List<String> columns, boolean isStrict) {
		this(conn, tableName, columns, isStrict, null);
	}

	public CSVCommonsLoader(PhoenixConnection conn, String tableName,
			List<String> columns, boolean isStrict, List<String> customMetaCharacters) {
		this.conn = conn;
		this.tableName = tableName;
		this.columns = columns;
		this.isStrict = isStrict;
		this.customMetaCharacters = customMetaCharacters;
		if (customMetaCharacters==null || customMetaCharacters.size()==0) {
			userSuppliedMetaCharacters=false;
		} else if (customMetaCharacters.size()==3) {
			userSuppliedMetaCharacters=true;
		}
		else{
			throw new IllegalArgumentException(
					String.format("customMetaCharacters must have no elements or three elements. Supplied value is %s",
							buildStringFromList(customMetaCharacters)));
		}
		
		// implicit in the columns value.
		if (columns !=null && !columns.isEmpty()) {
			headerSource = PhoenixHeaderSource.SUPPLIED_BY_USER;
		}
		else if (columns != null && columns.isEmpty()) {
			headerSource = PhoenixHeaderSource.IN_LINE;
		}
		
		this.format = buildFormat();
	}
	
	public CSVFormat getFormat() {
		return format;
	}
	
	/**
     * default settings
     * delimiter = ',' 
     * quoteChar = '"',
     * escape = null
     * recordSeparator = CRLF, CR, or LF
     * ignore empty lines allows the last data line to have a recordSeparator
	 * 
	 * @return CSVFormat based on constructor settings.
	 */
	private CSVFormat buildFormat() {
        CSVFormat format = CSVFormat.DEFAULT
                .withIgnoreEmptyLines(true);
        if (userSuppliedMetaCharacters) {
        	// list error checking handled in constructor above. 
        	// use 0 to keep default setting
        	String delimiter = customMetaCharacters.get(0);
        	String quote = customMetaCharacters.get(1);
        	String escape = customMetaCharacters.get(2);
        	
        	if (!"0".equals(delimiter)) {
        		format = format.withDelimiter(getCustomMetaCharacter(delimiter));
        	}
        	if (!"0".equals(quote)) {
        		format = format.withQuoteChar(getCustomMetaCharacter(quote));
        	}
        	if (!"0".equals(quote)) {
        		format = format.withEscape(getCustomMetaCharacter(escape));
        	}
        	
        }
        switch(headerSource) {
        case FROM_TABLE:
      	  // obtain headers from table, so format should not expect a header.
      	break;
        case IN_LINE:
      	  // an empty string array triggers csv loader to grab the first line as the header
      	  format = format.withHeader(new String[0]);
      	break;
        case SUPPLIED_BY_USER:
      	  // a populated string array supplied by the user
      	  format = format.withHeader(columns.toArray(new String[columns.size()]));
      	break;
      default:
      	throw new RuntimeException("Header source was unable to be inferred.");
      		
      }
        return format;
	}
        

    public char getCustomMetaCharacter(String field) {
        if(this.ctrlTable.containsKey(field)) {
            return this.ctrlTable.get(field);
        } else {
            return field.charAt(0);
        }
    }

    /**
	 * Upserts data from CSV file. 
	 * 
	 * Data is batched up based on connection batch size. 
	 * Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. 
	 * 
	 * The constructor determines the format for the CSV files.
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public void upsert(String fileName) throws Exception {
		CSVParser parser = CSVParser.parse(new File(fileName), 
				format); 
		upsert(parser);
	}
	
	public void upsert(Reader reader) throws Exception {
		CSVParser parser = new CSVParser(reader,format); 
		upsert(parser);
	}

	private static <T> String buildStringFromList(List<T> list) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (T val : list) {
			if (first) {
				sb.append((val==null?"null":val.toString()));
				first = false;
			} else {
				sb.append(", " + (val==null?"null":val.toString()));
			}
		}
		return sb.toString();
	}

	/**
	 * Data is batched up based on connection batch size. 
	 * Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. 
	 * 
	 * The format is determined by the supplied parser. 

	 * @param parser
	 *            CSVParser instance
	 * @throws Exception
	 */
	public void upsert(CSVParser parser) throws Exception {
		List<String> columns = this.columns;
		switch (headerSource) {
		case FROM_TABLE:
			System.out.println(String.format("csv columns from database."));
			break;
		case IN_LINE: 
			columns = new ArrayList<String>(); 
			for (String colName : parser.getHeaderMap().keySet()) {
				columns.add(colName); // iterates in column order
			}
			System.out.println(String.format("csv columns from header line. length=%s, %s",
					columns.size(), buildStringFromList(columns)));
			break;
		case SUPPLIED_BY_USER:
			System.out.println(String.format("csv columns from user. length=%s, %s",
					columns.size(), buildStringFromList(columns)));
			break;
		default:
			throw new IllegalStateException("parser has unknown column source.");
		}
		ColumnInfo[] columnInfo = generateColumnInfo(columns);
		System.out.println(String.format("phoenix columnInfo length=%s, %s",
				columnInfo.length,
				buildStringFromList(Arrays.asList(columnInfo))));
		PreparedStatement stmt = null;
		PreparedStatement[] stmtCache = null;
		if (columns == null) {
			stmtCache = new PreparedStatement[columnInfo.length];
		} else {
			String upsertStatement = QueryUtil.constructUpsertStatement(
					columnInfo, tableName, columnInfo.length
							- unfoundColumnCount);
			System.out.println(String.format("prepared statement %s",
					upsertStatement));
			stmt = conn.prepareStatement(upsertStatement);
		}
		int rowCount = 0;
		int upsertBatchSize = conn.getMutateBatchSize();
		boolean wasAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			Object upsertValue = null;
			long start = System.currentTimeMillis();

			int count = 0;

			// Upsert data based on SqlType of each column
			for (CSVRecord nextRecord : parser) {
				count++;
				//TODO expose progress monitor setting
				if (count % 1000 == 0) { 
					System.out.println(String.format(
							"processing line line %s, %s=%s", count,
							columnInfo[0].getColumnName(), nextRecord.get(0)));
				}
				if (columns == null) {
					stmt = stmtCache[nextRecord.size() - 1];
					if (stmt == null) {
						String upsertStatement = QueryUtil
								.constructUpsertStatement(columnInfo,
										tableName, nextRecord.size());
						stmt = conn.prepareStatement(upsertStatement);
						stmtCache[nextRecord.size() - 1] = stmt;
					}
				}

				// skip inconsistent line,
				if (!nextRecord.isConsistent()) { 
					System.out.println(String.format(
							"Unable to process line number %s"
									+ ", line columns %s"
									+ ", expected columns %s\n%s", count,
							nextRecord.size(), columnInfo.length,
							nextRecord.toString()));
					continue;
				}
				for (int index = 0; index < columnInfo.length; index++) {
					if (columnInfo[index] == null) {
						continue;
					}
					String fieldValue = nextRecord.get(index);
					Integer info = columnInfo[index].getSqlType();
					upsertValue = convertTypeSpecificValue(fieldValue, info);
					if (upsertValue != null) {
						stmt.setObject(index + 1, upsertValue,
								columnInfo[index].getSqlType());
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
			System.out.println("CSV Upsert complete. " + rowCount
					+ " rows upserted");
			System.out.println("Time: " + elapsedDuration + " sec(s)\n");
		} finally {
			if (stmt != null) {
				stmt.close();
			}
			// release reader resources.
			if (parser != null) {
				parser.close();
			}
			if (wasAutoCommit)
				conn.setAutoCommit(true);
		}
	}

	/**
	 * Gets CSV string input converted to correct type
	 */
	private Object convertTypeSpecificValue(String s, Integer sqlType)
			throws Exception {
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
		Map<String, Integer> columnNameToTypeMap = Maps.newLinkedHashMap();
		DatabaseMetaData dbmd = conn.getMetaData();
		// TODO: escape wildcard characters here because we don't want that
		// behavior here
		String escapedTableName = StringUtil.escapeLike(tableName);
		String[] schemaAndTable = escapedTableName.split("\\.");
		ResultSet rs = null;
		try {
			rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? ""
					: schemaAndTable[0]),
					(schemaAndTable.length == 1 ? escapedTableName
							: schemaAndTable[1]), null);
			while (rs.next()) {
				columnNameToTypeMap.put(
						rs.getString(QueryUtil.COLUMN_NAME_POSITION),
						rs.getInt(QueryUtil.DATA_TYPE_POSITION));
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		ColumnInfo[] columnType;
		if (columns == null) {
			int i = 0;
			columnType = new ColumnInfo[columnNameToTypeMap.size()];
			for (Map.Entry<String, Integer> entry : columnNameToTypeMap
					.entrySet()) {
				columnType[i++] = new ColumnInfo(entry.getKey(),
						entry.getValue());
			}
		} else {
			// Leave "null" as indication to skip b/c it doesn't exist
			columnType = new ColumnInfo[columns.size()];
			for (int i = 0; i < columns.size(); i++) {
				String columnName = SchemaUtil.normalizeIdentifier(columns.get(
						i).trim());
				Integer sqlType = columnNameToTypeMap.get(columnName);
				if (sqlType == null) {
					if (isStrict) {
						throw new SQLExceptionInfo.Builder(
								SQLExceptionCode.COLUMN_NOT_FOUND)
								.setColumnName(columnName)
								.setTableName(tableName).build()
								.buildException();
					}
					unfoundColumnCount++;
				} else {
					columnType[i] = new ColumnInfo(columnName, sqlType);
				}
			}
			if (unfoundColumnCount == columns.size()) {
				throw new SQLExceptionInfo.Builder(
						SQLExceptionCode.COLUMN_NOT_FOUND)
						.setColumnName(
								Arrays.toString(columns.toArray(new String[0])))
						.setTableName(tableName).build().buildException();
			}
		}
		return columnType;
	}
}
