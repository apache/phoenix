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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TableAuthManager;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;

public class CompatPermissionUtil {

    public static void stopAccessChecker(AccessChecker accessChecker) throws IOException {
        if (accessChecker.getAuthManager() != null) {
            TableAuthManager.release(accessChecker.getAuthManager());
        }
    }

    public static String getUserFromUP(UserPermission userPermission) {
        return Bytes.toString(userPermission.getUser());
    }

    public static Permission getPermissionFromUP(UserPermission userPermission) {
        return userPermission;
    }

    public static boolean userHasAccess(AccessChecker accessChecker, User user, TableName table,
            Permission.Action action) {
        return accessChecker.getAuthManager().userHasAccess(user, table, action);
    }

    public static boolean groupHasAccess(AccessChecker accessChecker, String group, TableName table,
            Permission.Action action) {
        return accessChecker.getAuthManager().groupHasAccess(group, table, action);
    }
}
