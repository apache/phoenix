package org.apache.phoenix.schema;

public class ParentTableNotFoundException extends TableNotFoundException {
	private static final long serialVersionUID = 1L;
	private final String parentTableName;

	public ParentTableNotFoundException(String parentTableName, String tableName) {
		super(tableName);
		this.parentTableName = parentTableName;
	}

}
