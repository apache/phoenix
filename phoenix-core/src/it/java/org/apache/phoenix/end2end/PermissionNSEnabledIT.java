/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PermissionNSEnabledIT extends BasePermissionsIT {

    public PermissionNSEnabledIT() throws Exception {
        super(true);
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        BasePermissionsIT.initCluster(true);
    }

    @Test
    public void testSchemaPermissions() throws Throwable{
        try {
            grantSystemTableAccess();
            final String schemaName = "S_" + generateUniqueName();
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        AccessControlClient.grant(getUtility().getConnection(), regularUser1.getShortName(),
                                Permission.Action.ADMIN);
                    } catch (Throwable e) {
                        if (e instanceof Exception) {
                            throw (Exception)e;
                        } else {
                            throw new Exception(e);
                        }
                    }
                    return null;
                }
            });
            verifyAllowed(createSchema(schemaName), regularUser1);
            // Unprivileged user cannot drop a schema
            verifyDenied(dropSchema(schemaName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(createSchema(schemaName), AccessDeniedException.class, unprivilegedUser);

            verifyAllowed(dropSchema(schemaName), regularUser1);
        } finally {
            revokeAll();
        }
    }

    @Test
    public void testLowerCaseSchemaPermissions() throws Throwable{
        try {
            grantSystemTableAccess();
            final String schemaName = "\"" + ("S_" + generateUniqueName()).toLowerCase() + "\"";
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        AccessControlClient.grant(getUtility().getConnection(), regularUser1.getShortName(),
                                Permission.Action.ADMIN);
                    } catch (Throwable e) {
                        if (e instanceof Exception) {
                            throw (Exception)e;
                        } else {
                            throw new Exception(e);
                        }
                    }
                    return null;
                }
            });
            verifyAllowed(createSchema(schemaName), regularUser1);
            // Unprivileged user cannot drop a schema
            verifyDenied(dropSchema(schemaName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(createSchema(schemaName), AccessDeniedException.class, unprivilegedUser);

            verifyAllowed(dropSchema(schemaName), regularUser1);
        } finally {
            revokeAll();
        }
    }

    @Test
    public void testConnectionCreationFailsWhenNoExecPermsOnSystemCatalog() throws Throwable {
        try {
            grantSystemTableAccess();
            superUser1.runAs((PrivilegedExceptionAction<Object>) () -> {
                TableName systemCatalogTableName =
                        TableName.valueOf(SchemaUtil.getPhysicalHBaseTableName(
                                SYSTEM_SCHEMA_NAME, SYSTEM_CATALOG_TABLE, true).getString());
                try {
                    // Revoke Exec permissions for SYSTEM CATALOG for the unprivileged user
                    AccessControlClient.revoke(getUtility().getConnection(), systemCatalogTableName,
                            unprivilegedUser.getShortName(), null, null, Permission.Action.EXEC);
                } catch (Throwable t) {
                    if (t instanceof Exception) {
                        throw (Exception)t;
                    } else {
                        throw new Exception(t);
                    }
                }
                return null;
            });
            unprivilegedUser.runAs((PrivilegedExceptionAction<Void>) () -> {
                try (Connection ignored = getConnection()) {
                    // We expect this to throw a wrapped AccessDeniedException.
                    fail("Should have failed with a wrapped AccessDeniedException");
                } catch (Throwable ex) {
                    assertTrue("Should not get an incompatible jars exception",
                            ex instanceof SQLException && ((SQLException)ex).getErrorCode() !=
                                    SQLExceptionCode.INCOMPATIBLE_CLIENT_SERVER_JAR.getErrorCode());
                    assertTrue("Expected a wrapped AccessDeniedException",
                            ex.getCause() instanceof AccessDeniedException);
                }
                return null;
            });
        } finally {
            revokeAll();
        }
    }

    // After PHOENIX-4810, a user requires Exec permissions on SYSTEM.CHILD_LINK to create views
    // since the user must invoke the ChildLinkMetaDataEndpoint to create parent->child links
    @Test
    public void testViewCreationFailsWhenNoExecPermsOnSystemChildLink() throws Throwable {
        try {
            grantSystemTableAccess();
            TableName systemChildLink = TableName.valueOf(SchemaUtil.getPhysicalHBaseTableName(
                    SYSTEM_SCHEMA_NAME, SYSTEM_CHILD_LINK_TABLE, true).getString());
            final String schemaName = "S_" + generateUniqueName();
            final String tableName = "T_" + generateUniqueName();
            final String fullTableName = schemaName + "." + tableName;
            final String viewName = "V_" + generateUniqueName();
            verifyAllowed(createSchema(schemaName), superUser1);
            verifyAllowed(createTable(fullTableName), superUser1);

            superUser1.runAs(new PrivilegedExceptionAction<Object>() {
                @Override public Object run() throws Exception {
                    try {
                        // Revoke Exec permissions for SYSTEM CHILD_LINK for the unprivileged user
                        AccessControlClient.revoke(getUtility().getConnection(), systemChildLink,
                                unprivilegedUser.getShortName(), null, null,
                                Permission.Action.EXEC);

                        // Grant read and exec permissions to the user on the parent table so it
                        // doesn't fail to getTable when resolving the parent
                        PermissionNSEnabledIT.this.grantPermissions(unprivilegedUser.getShortName(),
                                Collections.singleton(SchemaUtil
                                        .getPhysicalHBaseTableName(schemaName, tableName, true)
                                        .getString()), Permission.Action.READ,
                                Permission.Action.EXEC);
                    } catch (Throwable t) {
                        if (t instanceof Exception) {
                            throw (Exception) t;
                        } else {
                            throw new Exception(t);
                        }
                    }
                    return null;
                }
            });

            // Adding parent->child links fails for the unprivileged user thus failing view creation
            verifyDenied(createView(viewName, fullTableName), AccessDeniedException.class,
                    unprivilegedUser);

            superUser1.runAs(new PrivilegedExceptionAction<Object>() {
                @Override public Object run() throws Exception {
                    try {
                        // Grant Exec permissions for SYSTEM CHILD_LINK for the unprivileged user
                        PermissionNSEnabledIT.this.grantPermissions(unprivilegedUser.getShortName(),
                                Collections.singleton(systemChildLink.getNameAsString()),
                                Permission.Action.EXEC);
                    } catch (Throwable t) {
                        if (t instanceof Exception) {
                            throw (Exception) t;
                        } else {
                            throw new Exception(t);
                        }
                    }
                    return null;
                }
            });
            verifyAllowed(createView(viewName, fullTableName), unprivilegedUser);
        } finally {
            revokeAll();
        }
    }

}
