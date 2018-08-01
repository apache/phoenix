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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that verifies a user can read Phoenix tables with a minimal set of permissions.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ChangePermissionsIT extends BasePermissionsIT {

    private static final Log LOG = LogFactory.getLog(ChangePermissionsIT.class);

    private static final String SCHEMA_NAME = "CHANGEPERMSSCHEMA";
    private static final String TABLE_NAME =
            ChangePermissionsIT.class.getSimpleName().toUpperCase();
    private static final String FULL_TABLE_NAME = SCHEMA_NAME + "." + TABLE_NAME;
    private static final String IDX1_TABLE_NAME = TABLE_NAME + "_IDX1";
    private static final String IDX2_TABLE_NAME = TABLE_NAME + "_IDX2";
    private static final String IDX3_TABLE_NAME = TABLE_NAME + "_IDX3";
    private static final String LOCAL_IDX1_TABLE_NAME = TABLE_NAME + "_LIDX1";
    private static final String VIEW1_TABLE_NAME = TABLE_NAME + "_V1";
    private static final String VIEW2_TABLE_NAME = TABLE_NAME + "_V2";

    public ChangePermissionsIT(boolean isNamespaceMapped) throws Exception {
        super(isNamespaceMapped);
    }

    private void grantSystemTableAccess(User superUser, User... users) throws Exception {
        for(User user : users) {
            if(isNamespaceMapped) {
                verifyAllowed(grantPermissions("RX", user, QueryConstants.SYSTEM_SCHEMA_NAME, true), superUser);
            } else {
                verifyAllowed(grantPermissions("RX", user, PHOENIX_SYSTEM_TABLES_IDENTIFIERS, false), superUser);
            }
            verifyAllowed(grantPermissions("W", user, SYSTEM_SEQUENCE_IDENTIFIER, false), superUser);
        }
    }

    private void revokeSystemTableAccess(User superUser, User... users) throws Exception {
        for(User user : users) {
            if(isNamespaceMapped) {
                verifyAllowed(revokePermissions(user, QueryConstants.SYSTEM_SCHEMA_NAME, true), superUser);
            } else {
                verifyAllowed(revokePermissions(user, PHOENIX_SYSTEM_TABLES_IDENTIFIERS, false), superUser);
            }
            verifyAllowed(revokePermissions(user, SYSTEM_SEQUENCE_IDENTIFIER, false), superUser);
        }
    }

    /**
     * Verify that READ and EXECUTE permissions are required on SYSTEM tables to get a Phoenix Connection
     * Tests grant revoke permissions per user 1. if NS enabled -> on namespace 2. If NS disabled -> on tables
     */
    @Test
    public void testRXPermsReqdForPhoenixConn() throws Exception {

        startNewMiniCluster();

        if(isNamespaceMapped) {
            // NS is enabled, CQSI tries creating SYSCAT, we get NamespaceNotFoundException exception for "SYSTEM" NS
            // We create custom ADE and throw it (and ignore NamespaceNotFoundException)
            // This is because we didn't had CREATE perms to create "SYSTEM" NS
            verifyDenied(getConnectionAction(), AccessDeniedException.class, regularUser1);
        } else {
            // NS is disabled, CQSI tries creating SYSCAT, Two cases here
            // 1. First client ever --> Gets ADE, runs client server compatibility check again and gets TableNotFoundException since SYSCAT doesn't exist
            // 2. Any other client --> Gets ADE, runs client server compatibility check again and gets AccessDeniedException since it doesn't have EXEC perms
            verifyDenied(getConnectionAction(), TableNotFoundException.class, regularUser1);
        }

        // Phoenix Client caches connection per user
        // If we grant permissions, get a connection and then revoke it, we can still get the cached connection
        // However it will fail for other read queries
        // Thus this test grants and revokes for 2 users, so that both functionality can be tested.
        grantSystemTableAccess(superUser1, regularUser1, regularUser2);
        verifyAllowed(getConnectionAction(), regularUser1);
        revokeSystemTableAccess(superUser1, regularUser2);
        verifyDenied(getConnectionAction(), AccessDeniedException.class, regularUser2);
    }

    /**
     * Superuser grants admin perms to user1, who will in-turn grant admin perms to user2
     * Not affected with namespace props
     * Tests grant revoke permissions on per user global level
     */
    @Test
    public void testSuperUserCanChangePerms() throws Exception {

        startNewMiniCluster();

        // Grant System Table access to all users, else they can't create a Phoenix connection
        grantSystemTableAccess(superUser1, regularUser1, regularUser2, unprivilegedUser);

        verifyAllowed(grantPermissions("A", regularUser1), superUser1);

        verifyAllowed(readTableWithoutVerification(PhoenixDatabaseMetaData.SYSTEM_CATALOG), regularUser1);
        verifyAllowed(grantPermissions("A", regularUser2), regularUser1);

        verifyAllowed(revokePermissions(regularUser1), superUser1);
        verifyDenied(grantPermissions("A", regularUser3), AccessDeniedException.class, regularUser1);

        // Don't grant ADMIN perms to unprivilegedUser, thus unprivilegedUser is unable to control other permissions.
        verifyAllowed(getConnectionAction(), unprivilegedUser);
        verifyDenied(grantPermissions("ARX", regularUser4), AccessDeniedException.class, unprivilegedUser);
    }

    /**
     * Test to verify READ permissions on table, indexes and views
     * Tests automatic grant revoke of permissions per user on a table
     */
    @Test
    public void testReadPermsOnTableIndexAndView() throws Exception {

        startNewMiniCluster();

        grantSystemTableAccess(superUser1, regularUser1, regularUser2, unprivilegedUser);

        // Create new schema and grant CREATE permissions to a user
        if(isNamespaceMapped) {
            verifyAllowed(createSchema(SCHEMA_NAME), superUser1);
            verifyAllowed(grantPermissions("C", regularUser1, SCHEMA_NAME, true), superUser1);
        } else {
            verifyAllowed(grantPermissions("C", regularUser1, surroundWithDoubleQuotes(QueryConstants.HBASE_DEFAULT_SCHEMA_NAME), true), superUser1);
        }

        // Create new table. Create indexes, views and view indexes on top of it. Verify the contents by querying it
        verifyAllowed(createTable(FULL_TABLE_NAME), regularUser1);
        verifyAllowed(readTable(FULL_TABLE_NAME), regularUser1);
        verifyAllowed(createIndex(IDX1_TABLE_NAME, FULL_TABLE_NAME), regularUser1);
        verifyAllowed(createIndex(IDX2_TABLE_NAME, FULL_TABLE_NAME), regularUser1);
        verifyAllowed(createLocalIndex(LOCAL_IDX1_TABLE_NAME, FULL_TABLE_NAME), regularUser1);
        verifyAllowed(createView(VIEW1_TABLE_NAME, FULL_TABLE_NAME), regularUser1);
        verifyAllowed(createIndex(IDX3_TABLE_NAME, VIEW1_TABLE_NAME), regularUser1);

        // RegularUser2 doesn't have any permissions. It can get a PhoenixConnection
        // However it cannot query table, indexes or views without READ perms
        verifyAllowed(getConnectionAction(), regularUser2);
        verifyDenied(readTable(FULL_TABLE_NAME), AccessDeniedException.class, regularUser2);
        verifyDenied(readTable(FULL_TABLE_NAME, IDX1_TABLE_NAME), AccessDeniedException.class, regularUser2);
        verifyDenied(readTable(VIEW1_TABLE_NAME), AccessDeniedException.class, regularUser2);
        verifyDenied(readTableWithoutVerification(SCHEMA_NAME + "." + IDX1_TABLE_NAME), AccessDeniedException.class, regularUser2);

        // Grant READ permissions to RegularUser2 on the table
        // Permissions should propagate automatically to relevant physical tables such as global index and view index.
        verifyAllowed(grantPermissions("RX", regularUser2, FULL_TABLE_NAME, false), regularUser1);
        // Granting permissions directly to index tables should fail
        verifyDenied(grantPermissions("W", regularUser2, SCHEMA_NAME + "." + IDX1_TABLE_NAME, false), AccessDeniedException.class, regularUser1);
        // Granting permissions directly to views should fail. We expect TableNotFoundException since VIEWS are not physical tables
        verifyDenied(grantPermissions("W", regularUser2, SCHEMA_NAME + "." + VIEW1_TABLE_NAME, false), TableNotFoundException.class, regularUser1);

        // Verify that all other access are successful now
        verifyAllowed(readTable(FULL_TABLE_NAME), regularUser2);
        verifyAllowed(readTable(FULL_TABLE_NAME, IDX1_TABLE_NAME), regularUser2);
        verifyAllowed(readTable(FULL_TABLE_NAME, IDX2_TABLE_NAME), regularUser2);
        verifyAllowed(readTable(FULL_TABLE_NAME, LOCAL_IDX1_TABLE_NAME), regularUser2);
        verifyAllowed(readTableWithoutVerification(SCHEMA_NAME + "." + IDX1_TABLE_NAME), regularUser2);
        verifyAllowed(readTable(VIEW1_TABLE_NAME), regularUser2);
        verifyAllowed(readMultiTenantTableWithIndex(VIEW1_TABLE_NAME), regularUser2);

        // Revoke READ permissions to RegularUser2 on the table
        // Permissions should propagate automatically to relevant physical tables such as global index and view index.
        verifyAllowed(revokePermissions(regularUser2, FULL_TABLE_NAME, false), regularUser1);
        // READ query should fail now
        verifyDenied(readTable(FULL_TABLE_NAME), AccessDeniedException.class, regularUser2);
        verifyDenied(readTableWithoutVerification(SCHEMA_NAME + "." + IDX1_TABLE_NAME), AccessDeniedException.class, regularUser2);

    }

    /**
     * Verifies permissions for users present inside a group
     */
    @Test
    public void testGroupUserPerms() throws Exception {

        startNewMiniCluster();

        if(isNamespaceMapped) {
            verifyAllowed(createSchema(SCHEMA_NAME), superUser1);
        }
        verifyAllowed(createTable(FULL_TABLE_NAME), superUser1);

        // Grant SYSTEM table access to GROUP_SYSTEM_ACCESS and regularUser1
        verifyAllowed(grantPermissions("RX", GROUP_SYSTEM_ACCESS, PHOENIX_SYSTEM_TABLES_IDENTIFIERS, false), superUser1);
        grantSystemTableAccess(superUser1, regularUser1);

        // Grant Permissions to Groups (Should be automatically applicable to all users inside it)
        verifyAllowed(grantPermissions("ARX", GROUP_SYSTEM_ACCESS, FULL_TABLE_NAME, false), superUser1);
        verifyAllowed(readTable(FULL_TABLE_NAME), groupUser);

        // GroupUser is an admin and can grant perms to other users
        verifyDenied(readTable(FULL_TABLE_NAME), AccessDeniedException.class, regularUser1);
        verifyAllowed(grantPermissions("RX", regularUser1, FULL_TABLE_NAME, false), groupUser);
        verifyAllowed(readTable(FULL_TABLE_NAME), regularUser1);

        // Revoke the perms and try accessing data again
        verifyAllowed(revokePermissions(GROUP_SYSTEM_ACCESS, FULL_TABLE_NAME, false), superUser1);
        verifyDenied(readTable(FULL_TABLE_NAME), AccessDeniedException.class, groupUser);
    }

    /**
     * Tests permissions for MultiTenant Tables and view index tables
     */
    @Test
    public void testMultiTenantTables() throws Exception {

        startNewMiniCluster();

        grantSystemTableAccess(superUser1, regularUser1, regularUser2, regularUser3);

        if(isNamespaceMapped) {
            verifyAllowed(createSchema(SCHEMA_NAME), superUser1);
            verifyAllowed(grantPermissions("C", regularUser1, SCHEMA_NAME, true), superUser1);
        } else {
            verifyAllowed(grantPermissions("C", regularUser1, surroundWithDoubleQuotes(QueryConstants.HBASE_DEFAULT_SCHEMA_NAME), true), superUser1);
        }

        // Create MultiTenant Table (View Index Table should be automatically created)
        // At this point, the index table doesn't contain any data
        verifyAllowed(createMultiTenantTable(FULL_TABLE_NAME), regularUser1);

        // RegularUser2 doesn't have access yet, RegularUser1 should have RWXCA on the table
        verifyDenied(readMultiTenantTableWithoutIndex(FULL_TABLE_NAME), AccessDeniedException.class, regularUser2);

        // Grant perms to base table (Should propagate to View Index as well)
        verifyAllowed(grantPermissions("RX", regularUser2, FULL_TABLE_NAME, false), regularUser1);
        // Try reading full table
        verifyAllowed(readMultiTenantTableWithoutIndex(FULL_TABLE_NAME), regularUser2);

        // Create tenant specific views on the table using tenant specific Phoenix Connection
        verifyAllowed(createView(VIEW1_TABLE_NAME, FULL_TABLE_NAME, "o1"), regularUser1);
        verifyAllowed(createView(VIEW2_TABLE_NAME, FULL_TABLE_NAME, "o2"), regularUser1);

        // Create indexes on those views using tenant specific Phoenix Connection
        // It is not possible to create indexes on tenant specific views without tenant connection
        verifyAllowed(createIndex(IDX1_TABLE_NAME, VIEW1_TABLE_NAME, "o1"), regularUser1);
        verifyAllowed(createIndex(IDX2_TABLE_NAME, VIEW2_TABLE_NAME, "o2"), regularUser1);

        // Read the tables as regularUser2, with and without the use of Index table
        // If perms are propagated correctly, then both of them should work
        // The test checks if the query plan uses the index table by searching for "_IDX_" string
        // _IDX_ is the prefix used with base table name to derieve the name of view index table
        verifyAllowed(readMultiTenantTableWithIndex(VIEW1_TABLE_NAME, "o1"), regularUser2);
        verifyAllowed(readMultiTenantTableWithoutIndex(VIEW2_TABLE_NAME, "o2"), regularUser2);
    }

    /**
     * Grant RX permissions on the schema to regularUser1,
     * Creating view on a table with that schema by regularUser1 should be allowed
     */
    @Test
    public void testCreateViewOnTableWithRXPermsOnSchema() throws Exception {

        startNewMiniCluster();
        grantSystemTableAccess(superUser1, regularUser1, regularUser2, regularUser3);

        if(isNamespaceMapped) {
            verifyAllowed(createSchema(SCHEMA_NAME), superUser1);
            verifyAllowed(createTable(FULL_TABLE_NAME), superUser1);
            verifyAllowed(grantPermissions("RX", regularUser1, SCHEMA_NAME, true), superUser1);
        } else {
            verifyAllowed(createTable(FULL_TABLE_NAME), superUser1);
            verifyAllowed(grantPermissions("RX", regularUser1, surroundWithDoubleQuotes(QueryConstants.HBASE_DEFAULT_SCHEMA_NAME), true), superUser1);
        } 
        verifyAllowed(createView(VIEW1_TABLE_NAME, FULL_TABLE_NAME), regularUser1);
    }
}
