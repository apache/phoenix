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

import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.phoenix.compat.hbase.CompatUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class PermissionsCacheIT extends BasePermissionsIT {

    public PermissionsCacheIT() throws Exception {
		super(true);
	}
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        BasePermissionsIT.initCluster(true);
    }

    @Test
    public void testPermissionsCachedWithAccessChecker() throws Throwable {
        if (!isNamespaceMapped) {
            return;
        }
        final String schema = generateUniqueName();
        final String tableName = generateUniqueName();
        final String phoenixTableName = SchemaUtil.getTableName(schema, tableName);
        try (Connection conn = getConnection()) {
            grantPermissions(regularUser1.getShortName(), PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES,
                Action.READ, Action.EXEC);
            grantPermissions(regularUser1.getShortName(), Collections.singleton("SYSTEM:SEQUENCE"),
                Action.WRITE, Action.READ, Action.EXEC);
            superUser1.runAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    try {
                        verifyAllowed(createSchema(schema), superUser1);
                        grantPermissions(regularUser1.getShortName(), schema, Action.CREATE);
                        grantPermissions(AuthUtil.toGroupEntry(GROUP_SYSTEM_ACCESS), schema,
                            Action.CREATE);
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
            verifyAllowed(createTable(phoenixTableName), regularUser1);
            HBaseTestingUtility utility = getUtility();
            Configuration conf = utility.getConfiguration();
            ZKWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
            String aclZnodeParent = conf.get("zookeeper.znode.acl.parent", "acl");
            String aclZNode = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, aclZnodeParent);
            String tableZNode = ZNodePaths.joinZNode(aclZNode, "@" + schema);
            byte[] data = ZKUtil.getData(zkw, tableZNode);
            ListMultimap<String, ? extends Permission> userPermissions =
                    CompatUtil.readPermissions(data, conf);
            assertTrue("User permissions not found in cache:",
                userPermissions.containsKey(regularUser1.getName()));
            List<? extends Permission> tablePermissions =
                    userPermissions.get(regularUser1.getName());
            for (Permission tablePerm : tablePermissions) {
                assertTrue("Table create permission don't exist", tablePerm.implies(Action.CREATE));
            }
        } catch (Exception e) {
            System.out.println("Exception occurred: " + e);
            throw e;
        }
    }

}
