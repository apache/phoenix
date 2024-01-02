/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import org.antlr.runtime.RecognitionException;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.util.SchemaUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * See PHOENIX-672, Use GRANT/REVOKE statements to assign or remove permissions for a user OR group on a table OR namespace
 * Permissions are managed by HBase using hbase:acl table, Allowed permissions are RWXCA
 */
public class ChangePermsStatement implements BindableStatement {

    private Permission.Action[] permsList;
    private TableName tableName;
    private String schemaName;
    private String name;
    // Grant/Revoke statements are differentiated based on this boolean
    private boolean isGrantStatement;

    public ChangePermsStatement(String permsString, boolean isSchemaName,
                                TableName tableName, String schemaName, boolean isGroupName, LiteralParseNode ugNode, boolean isGrantStatement) {
        // PHOENIX-672 HBase API doesn't allow to revoke specific permissions, hence this parameter will be ignored here.
        // To comply with SQL standards, we may support the user given permissions to revoke specific permissions in future.
        // GRANT permissions statement requires this parameter and the parsing will fail if it is not specified in SQL
        if(permsString != null) {
            Permission permission = new Permission(permsString.getBytes(StandardCharsets.UTF_8));
            permsList = permission.getActions();
        }
        if(isSchemaName) {
            this.schemaName = SchemaUtil.normalizeIdentifier(schemaName);
        } else {
            this.tableName = tableName;
        }
        name = SchemaUtil.normalizeLiteral(ugNode);
        name = isGroupName ? AuthUtil.toGroupEntry(name) : name;
        this.isGrantStatement = isGrantStatement;
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

    public boolean isGrantStatement() {
        return isGrantStatement;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer = this.isGrantStatement() ? buffer.append("GRANT ") : buffer.append("REVOKE ");
        buffer.append("permissions requested for user/group: " + this.getName());
        if (this.getSchemaName() != null) {
            buffer.append(" for Schema: " + this.getSchemaName());
        } else if (this.getTableName() != null) {
            buffer.append(" for Table: " + this.getTableName());
        }
        buffer.append(" Permissions: " + Arrays.toString(this.getPermsList()));
        return buffer.toString();
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
