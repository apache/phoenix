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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatPermissionUtil {
    
    public static void stopAccessChecker(AccessChecker accessChecker) throws IOException {
        accessChecker.stop();
    }

    public static void initializeSuperUser() throws IOException {
        //Not used for 2.2
    }
    
    public static String getUserFromUP(UserPermission userPermission) {
        return userPermission.getUser();
    }
    
    public static Permission getPermissionFromUP(UserPermission userPermission) {
        return userPermission.getPermission();
    }
    
    public static boolean implies(UserPermission userPermission, Permission.Action action) {
        return userPermission.getPermission().implies(action);
    }
    
    public static Action[] getActions(UserPermission userPermission) {
        return userPermission.getPermission().getActions();
    }

    public static boolean userHasAccess(AccessChecker accessChecker, User user, TableName table, Permission.Action action) {
        //This also checks for group access
        return accessChecker.getAuthManager().authorizeUserTable(user, table, action);
    }

    public static boolean groupHasAccess(AccessChecker accessChecker, String group, TableName table, Permission.Action action) {
        //We are calling it only after userHasAccess() has failed.
        //In Hbase 2.2+, userHasAccess has already checked for group access, so no point in repeating
        return false;
    }
}
