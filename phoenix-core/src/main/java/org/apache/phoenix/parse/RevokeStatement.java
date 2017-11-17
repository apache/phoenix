package org.apache.phoenix.parse;

import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.util.SchemaUtil;

public class RevokeStatement implements BindableStatement {

    Permission.Action[] permsList;
    TableName tableName;
    String schemaName;
    String name;
    boolean isGroupName;

    public RevokeStatement(LiteralParseNode permsNode, boolean isSchemaName, TableName tableName, String schemaName, boolean isGroupName, LiteralParseNode ugNode) {
        // PHOENIX-672 HBase API doesn't allow to revoke specific permissions, hence this parameter will be ignored here.
        // To comply with SQL standards, we may support the user given permissions to revoke specific permissions in future.
        if(permsNode != null) {
            Permission permission = new Permission(SchemaUtil.normalizeLiteral(permsNode).getBytes());
            permsList = permission.getActions();
        }
        if(isSchemaName) {
            this.schemaName = SchemaUtil.normalizeIdentifier(schemaName);
        } else {
            this.tableName = tableName;
        }
        this.isGroupName = isGroupName;
        name = SchemaUtil.normalizeLiteral(ugNode);
        name = this.isGroupName ? AuthUtil.toGroupEntry(name) : name;

    }

    public String getSchemaName() {
        return schemaName;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getName() {
        return name;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    @Override
    public PhoenixStatement.Operation getOperation() {
        return PhoenixStatement.Operation.ADMIN;
    }
}
