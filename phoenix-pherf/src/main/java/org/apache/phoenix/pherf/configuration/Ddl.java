package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlAttribute;

public class Ddl {
	private String statement;
	private String tableName;
	
	public Ddl() {
	}
	
	public Ddl(String statement, String tableName) {
		this.statement = statement;
		this.tableName = tableName;
	}
	
	/**
	 * DDL
	 * @return
	 */
	@XmlAttribute
	public String getStatement() {
		return statement;
	}
	public void setStatement(String statement) {
		this.statement = statement;
	}
	
	/**
	 * Table name used in the DDL
	 * @return
	 */
	@XmlAttribute
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String toString(){
		if (statement.contains("?")) {
			return statement.replace("?", tableName);
		} else {
			return statement;
		}
		
	}
	
}
