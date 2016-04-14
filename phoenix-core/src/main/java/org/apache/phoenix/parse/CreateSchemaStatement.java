package org.apache.phoenix.parse;

public class CreateSchemaStatement extends MutableStatement {
	private final String schemaName;
	private final boolean ifNotExists;
	
	public CreateSchemaStatement(String schemaName,boolean ifNotExists) {
		this.schemaName=schemaName;
		this.ifNotExists = ifNotExists;
	}
	
	@Override
	public int getBindCount() {
		return 0;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public boolean isIfNotExists() {
		return ifNotExists;
	}

}
