package org.apache.phoenix.parse;

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;

public class DropSchemaStatement extends MutableStatement {
    private final String schemaName;
    private final boolean ifExists;
    private final boolean cascade;

    public DropSchemaStatement(String schemaName, boolean ifExists, boolean cascade) {
        this.schemaName = schemaName;
        this.ifExists = ifExists;
        this.cascade = cascade;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean ifExists() {
        return ifExists;
    }

    public boolean cascade() {
        return cascade;
    }

    @Override
    public Operation getOperation() {
        return Operation.DELETE;
    }

}
