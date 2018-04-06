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
package org.apache.phoenix.end2end;


import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.util.Collections;

import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test that verifies a user can read Phoenix tables with a minimal set of permissions.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TableDDLPermissionsIT extends BasePermissionsIT{

    public TableDDLPermissionsIT(boolean isNamespaceMapped) throws Exception {
        super(isNamespaceMapped);
    }
    
    private void grantSystemTableAccess() throws Exception{
        
        try (Connection conn = getConnection()) {
            if (isNamespaceMapped) {
                grantPermissions(regularUser1.getShortName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, Action.READ,
                        Action.EXEC);
                grantPermissions(unprivilegedUser.getShortName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                        Action.READ, Action.EXEC);
                grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                        Action.READ, Action.EXEC);
                // Local Index requires WRITE permission on SYSTEM.SEQUENCE TABLE.
                grantPermissions(regularUser1.getName(), Collections.singleton("SYSTEM:SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
                grantPermissions(unprivilegedUser.getName(), Collections.singleton("SYSTEM:SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
                
            } else {
                grantPermissions(regularUser1.getName(), PHOENIX_SYSTEM_TABLES, Action.READ, Action.EXEC);
                grantPermissions(unprivilegedUser.getName(), PHOENIX_SYSTEM_TABLES, Action.READ, Action.EXEC);
                grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), PHOENIX_SYSTEM_TABLES, Action.READ, Action.EXEC);
                // Local Index requires WRITE permission on SYSTEM.SEQUENCE TABLE.
                grantPermissions(regularUser1.getName(), Collections.singleton("SYSTEM.SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
                grantPermissions(unprivilegedUser.getName(), Collections.singleton("SYSTEM:SEQUENCE"), Action.WRITE,
                        Action.READ, Action.EXEC);
            }
        } catch (Throwable e) {
            if (e instanceof Exception) {
                throw (Exception)e;
            } else {
                throw new Exception(e);
            }
        }
    }

    @Test
    public void testSchemaPermissions() throws Throwable{

        if (!isNamespaceMapped) { return; }
        try {
            startNewMiniCluster();
            grantSystemTableAccess();
            final String schemaName = "TEST_SCHEMA_PERMISSION";
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        AccessControlClient.grant(getUtility().getConnection(), regularUser1.getShortName(),
                                Action.ADMIN);
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
    public void testAutomaticGrantWithIndexAndView() throws Throwable {
        startNewMiniCluster();
        final String schema = "TEST_INDEX_VIEW";
        final String tableName = "TABLE_DDL_PERMISSION_IT";
        final String phoenixTableName = schema + "." + tableName;
        final String indexName1 = tableName + "_IDX1";
        final String indexName2 = tableName + "_IDX2";
        final String lIndexName1 = tableName + "_LIDX1";
        final String viewName1 = schema+"."+tableName + "_V1";
        final String viewName2 = schema+"."+tableName + "_V2";
        final String viewName3 = schema+"."+tableName + "_V3";
        final String viewName4 = schema+"."+tableName + "_V4";
        final String viewIndexName1 = tableName + "_VIDX1";
        final String viewIndexName2 = tableName + "_VIDX2";
        grantSystemTableAccess();
        try {
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        verifyAllowed(createSchema(schema), superUser1);
                        //Neded Global ADMIN for flush operation during drop table
                        AccessControlClient.grant(getUtility().getConnection(),regularUser1.getName(), Action.ADMIN);
                        if (isNamespaceMapped) {
                            grantPermissions(regularUser1.getName(), schema, Action.CREATE);
                            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), schema, Action.CREATE);

                        } else {
                            grantPermissions(regularUser1.getName(),
                                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Action.CREATE);
                            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),
                                    NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), Action.CREATE);

                        }
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

            verifyAllowed(createTable(phoenixTableName), regularUser1);
            verifyAllowed(createIndex(indexName1, phoenixTableName), regularUser1);
            verifyAllowed(createView(viewName1, phoenixTableName), regularUser1);
            verifyAllowed(createLocalIndex(lIndexName1, phoenixTableName), regularUser1);
            verifyAllowed(createIndex(viewIndexName1, viewName1), regularUser1);
            verifyAllowed(createIndex(viewIndexName2, viewName1), regularUser1);
            verifyAllowed(createView(viewName4, viewName1), regularUser1);
            verifyAllowed(readTable(phoenixTableName), regularUser1);

            verifyDenied(createIndex(indexName2, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(createView(viewName2, phoenixTableName),AccessDeniedException.class,  unprivilegedUser);
            verifyDenied(createView(viewName3, viewName1), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(dropView(viewName1), AccessDeniedException.class, unprivilegedUser);
            
            verifyDenied(dropIndex(indexName1, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(dropTable(phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(rebuildIndex(indexName1, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(addColumn(phoenixTableName, "val1"), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(dropColumn(phoenixTableName, "val"), AccessDeniedException.class, unprivilegedUser);
            verifyDenied(addProperties(phoenixTableName, "GUIDE_POSTS_WIDTH", "100"), AccessDeniedException.class, unprivilegedUser);

            // Granting read permission to unprivileged user, now he should be able to create view but not index
            grantPermissions(unprivilegedUser.getShortName(),
                    Collections.singleton(
                            SchemaUtil.getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getString()),
                    Action.READ, Action.EXEC);
            grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS),
                    Collections.singleton(
                            SchemaUtil.getPhysicalHBaseTableName(schema, tableName, isNamespaceMapped).getString()),
                    Action.READ, Action.EXEC);
            verifyDenied(createIndex(indexName2, phoenixTableName), AccessDeniedException.class, unprivilegedUser);
            verifyAllowed(createView(viewName2, phoenixTableName), unprivilegedUser);
            verifyAllowed(createView(viewName3, viewName1), unprivilegedUser);
            
            // Grant create permission in namespace
            if (isNamespaceMapped) {
                grantPermissions(unprivilegedUser.getShortName(), schema, Action.CREATE);
            } else {
                grantPermissions(unprivilegedUser.getShortName(), NamespaceDescriptor.DEFAULT_NAMESPACE.getName(),
                        Action.CREATE);
            }

            // we should be able to read the data from another index as well to which we have not given any access to
            // this user
            verifyAllowed(createIndex(indexName2, phoenixTableName), unprivilegedUser);
            verifyAllowed(readTable(phoenixTableName, indexName1), unprivilegedUser);
            verifyAllowed(readTable(phoenixTableName, indexName2), unprivilegedUser);
            verifyAllowed(rebuildIndex(indexName2, phoenixTableName), unprivilegedUser);

            // data table user should be able to read new index
            verifyAllowed(rebuildIndex(indexName2, phoenixTableName), regularUser1);
            verifyAllowed(readTable(phoenixTableName, indexName2), regularUser1);

            verifyAllowed(readTable(phoenixTableName), regularUser1);
            verifyAllowed(rebuildIndex(indexName1, phoenixTableName), regularUser1);
            verifyAllowed(addColumn(phoenixTableName, "val1"), regularUser1);
            verifyAllowed(addProperties(phoenixTableName, "GUIDE_POSTS_WIDTH", "100"), regularUser1);
            verifyAllowed(dropView(viewName1), regularUser1);
            verifyAllowed(dropView(viewName2), regularUser1);
            verifyAllowed(dropColumn(phoenixTableName, "val1"), regularUser1);
            verifyAllowed(dropIndex(indexName2, phoenixTableName), regularUser1);
            verifyAllowed(dropIndex(indexName1, phoenixTableName), regularUser1);
            verifyAllowed(dropTable(phoenixTableName), regularUser1);

            // check again with super users
            verifyAllowed(createTable(phoenixTableName), superUser2);
            verifyAllowed(createIndex(indexName1, phoenixTableName), superUser2);
            verifyAllowed(createView(viewName1, phoenixTableName), superUser2);
            verifyAllowed(readTable(phoenixTableName), superUser2);
            verifyAllowed(dropView(viewName1), superUser2);
            verifyAllowed(dropTable(phoenixTableName), superUser2);

        } finally {
            revokeAll();
        }
    }

}
