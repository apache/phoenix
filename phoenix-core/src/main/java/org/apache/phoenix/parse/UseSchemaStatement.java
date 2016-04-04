package org.apache.phoenix.parse;

public class UseSchemaStatement extends MutableStatement {
    private final String schemaName;

    public UseSchemaStatement(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    public String getSchemaName() {
        return schemaName;
    }

}