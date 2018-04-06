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

import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that verifies a user can read Phoenix tables with a minimal set of permissions.
 * Uses HBase API directly to grant/revoke permissions
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SystemTablePermissionsIT extends BasePermissionsIT {

    private static final String TABLE_NAME =
        SystemTablePermissionsIT.class.getSimpleName().toUpperCase();

    public SystemTablePermissionsIT(boolean isNamespaceMapped) throws Exception {
        super(isNamespaceMapped);
    }

    @Test
    public void testSystemTablePermissions() throws Throwable {

        startNewMiniCluster();

        verifyAllowed(createTable(TABLE_NAME), superUser1);
        verifyAllowed(readTable(TABLE_NAME), superUser1);

        Set<String> tables = getHBaseTables();
        if(isNamespaceMapped) {
            assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
                    tables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));
        } else {
            assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
                    tables.containsAll(PHOENIX_SYSTEM_TABLES));
        }

        // Grant permission to the system tables for the unprivileged user
        superUser1.runAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    if(isNamespaceMapped) {
                        grantPermissions(regularUser1.getShortName(),
                                PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, Action.EXEC, Action.READ);
                    } else {
                        grantPermissions(regularUser1.getShortName(), PHOENIX_SYSTEM_TABLES,
                                Action.EXEC, Action.READ);
                    }
                    grantPermissions(regularUser1.getShortName(),
                        Collections.singleton(TABLE_NAME), Action.READ,Action.EXEC);
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        throw (Exception) e;
                    } else {
                        throw new Exception(e);
                    }
                }
                return null;
            }
        });

        // Make sure that the unprivileged user can now read the table
        verifyAllowed(readTable(TABLE_NAME), regularUser1);
    }
}
