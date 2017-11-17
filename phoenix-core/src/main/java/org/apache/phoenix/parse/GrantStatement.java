package org.apache.phoenix.parse;

import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.util.SchemaUtil;

public class GrantStatement implements BindableStatement {

    Permission.Action[] permsList;
    TableName tableName;
    String schemaName;
    String name;
    boolean isGroupName;

    public GrantStatement(LiteralParseNode permsNode, boolean isSchemaName, TableName tableName, String schemaName, boolean isGroupName, LiteralParseNode ugNode) {
        Permission permission = new Permission(SchemaUtil.normalizeLiteral(permsNode).getBytes());
        permsList = permission.getActions();
        if(isSchemaName) {
            this.schemaName = SchemaUtil.normalizeIdentifier(schemaName);
        } else {
            this.tableName = tableName;
        }
        this.isGroupName = isGroupName;
        name = SchemaUtil.normalizeLiteral(ugNode);
        name = this.isGroupName ? AuthUtil.toGroupEntry(name) : name;

    }

    public Permission.Action[] getPermsList() {
        return permsList;
    }

    public String getName() {
        return name;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    @Override
    public Operation getOperation() {
        return Operation.ADMIN;
    }
}
