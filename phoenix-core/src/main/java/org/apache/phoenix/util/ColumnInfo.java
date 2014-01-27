package org.apache.phoenix.util;

/**
 * ColumnInfo used to store Column Name and its associated PDataType
 */
public class ColumnInfo {
	private String columnName;
	private Integer sqlType;

	public ColumnInfo(String columnName, Integer sqlType) {
		this.columnName = columnName;
		this.sqlType = sqlType;
	}

	public String getColumnName() {
		return columnName;
	}

	public Integer getSqlType() {
		return sqlType;
	}
}	
