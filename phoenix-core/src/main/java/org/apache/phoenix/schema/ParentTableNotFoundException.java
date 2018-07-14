package org.apache.phoenix.schema;

import org.apache.phoenix.coprocessor.TableInfo;

public class ParentTableNotFoundException extends TableNotFoundException {
    private static final long serialVersionUID = 1L;
    private final byte[] parentTenantId;
    private final byte[] parentSchemaName;
    private final byte[] parentTableName;

    public ParentTableNotFoundException(TableInfo parentTableInfo, String tableName) {
        super(tableName);
        this.parentTenantId = parentTableInfo.getTenantId();
        this.parentSchemaName = parentTableInfo.getSchemaName();
        this.parentTableName = parentTableInfo.getTableName();
    }

    public byte[] getParentTenantId() {
        return parentTenantId;
    }

    public byte[] getParentSchemaName() {
        return parentSchemaName;
    }

    public byte[] getParentTableName() {
        return parentTableName;
    }

}
