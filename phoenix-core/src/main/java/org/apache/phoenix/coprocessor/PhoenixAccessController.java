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
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.BaseMasterAndRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AuthResult;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Lists;
import com.google.protobuf.RpcCallback;

public class PhoenixAccessController extends BaseMetaDataEndpointObserver {

    private PhoenixMetaDataControllerEnvironment env;
    private ArrayList<BaseMasterAndRegionObserver> accessControllers;
    private boolean accessCheckEnabled;
    private UserProvider userProvider;
    public static final Log LOG = LogFactory.getLog(PhoenixAccessController.class);
    private static final Log AUDITLOG =
            LogFactory.getLog("SecurityLogger."+PhoenixAccessController.class.getName());

    private List<BaseMasterAndRegionObserver> getAccessControllers() throws IOException {
        if (accessControllers == null) {
            synchronized (this) {
                if (accessControllers == null) {
                    accessControllers = new ArrayList<BaseMasterAndRegionObserver>();
                    RegionCoprocessorHost cpHost = this.env.getCoprocessorHost();
                    List<BaseMasterAndRegionObserver> coprocessors = cpHost
                            .findCoprocessors(BaseMasterAndRegionObserver.class);
                    for (BaseMasterAndRegionObserver cp : coprocessors) {
                        if (cp instanceof AccessControlService.Interface) {
                            accessControllers.add(cp);
                        }
                    }
                }
            }
        }
        return accessControllers;
    }

    @Override
    public void preGetTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName) throws IOException {
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preGetTableDescriptors(new ObserverContext<MasterCoprocessorEnvironment>(),
                    Lists.newArrayList(physicalTableName), Collections.<HTableDescriptor> emptyList());
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        Configuration conf = env.getConfiguration();
        this.accessCheckEnabled = conf.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        if (!this.accessCheckEnabled) {
            LOG.warn("PhoenixAccessController has been loaded with authorization checks disabled.");
        }
        if (env instanceof PhoenixMetaDataControllerEnvironment) {
            this.env = (PhoenixMetaDataControllerEnvironment)env;
        } else {
            throw new IllegalArgumentException(
                    "Not a valid environment, should be loaded by PhoenixMetaDataControllerEnvironment");
        }
        // set the user-provider.
        this.userProvider = UserProvider.instantiate(env.getConfiguration());
        // init superusers and add the server principal (if using security)
        // or process owner as default super user.
        Superusers.initialize(env.getConfiguration());
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {}

    @Override
    public void preCreateTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            Set<byte[]> familySet, Set<TableName> indexes) throws IOException {
        if (!accessCheckEnabled) { return; }
        
        if (tableType != PTableType.VIEW) {
            final HTableDescriptor htd = new HTableDescriptor(physicalTableName);
            for (byte[] familyName : familySet) {
                htd.addFamily(new HColumnDescriptor(familyName));
            }
            for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
                observer.preCreateTable(new ObserverContext<MasterCoprocessorEnvironment>(), htd, null);
            }
        }

        // Index and view require read access on parent physical table.
        Set<TableName> physicalTablesChecked = new HashSet<TableName>();
        if (tableType == PTableType.VIEW || tableType == PTableType.INDEX) {
            physicalTablesChecked.add(parentPhysicalTableName);
            requireAccess("Create" + tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
        }

        if (tableType == PTableType.VIEW) {
            
            Action[] requiredActions = { Action.READ, Action.EXEC };
            for (TableName index : indexes) {
                if (!physicalTablesChecked.add(index)) {
                    // skip check for local index as we have already check the ACLs above
                    // And for same physical table multiple times like view index table
                    continue;
                }

                User user = getActiveUser();
                List<UserPermission> permissionForUser = getPermissionForUser(
                        getUserPermissions(index.getNameAsString()), Bytes.toBytes(user.getShortName()));
                Set<Action> requireAccess = new HashSet<>();
                Set<Action> accessExists = new HashSet<>();
                if (permissionForUser != null) {
                    for (UserPermission userPermission : permissionForUser) {
                        for (Action action : Arrays.asList(requiredActions)) {
                            if (!userPermission.implies(action)) {
                                requireAccess.add(action);
                            }
                        }
                    }
                    if (!requireAccess.isEmpty()) {
                        for (UserPermission userPermission : permissionForUser) {
                            accessExists.addAll(Arrays.asList(userPermission.getActions()));
                        }

                    }
                } else {
                    requireAccess.addAll(Arrays.asList(requiredActions));
                }
                if (!requireAccess.isEmpty()) {
                    byte[] indexPhysicalTable = index.getName();
                    handleRequireAccessOnDependentTable("Create" + tableType, user.getName(),
                            TableName.valueOf(indexPhysicalTable), tableName, requireAccess, accessExists);
                }
            }

        }

        if (tableType == PTableType.INDEX) {
            // All the users who have READ access on data table should have access to Index table as well.
            // WRITE is needed for the index updates done by the user who has WRITE access on data table.
            // CREATE is needed during the drop of the table.
            // We are doing this because existing user while querying data table should not see access denied for the
            // new indexes.
            // TODO: confirm whether granting permission from coprocessor is a security leak.(currently it is done if
            // automatic grant is enabled explicitly by user in configuration
            // skip check for local index
            if (physicalTableName != null && !parentPhysicalTableName.equals(physicalTableName)
                    && !MetaDataUtil.isViewIndex(physicalTableName.getNameAsString())) {
                authorizeOrGrantAccessToUsers("Create" + tableType, parentPhysicalTableName,
                        Arrays.asList(Action.READ, Action.WRITE, Action.CREATE, Action.EXEC, Action.ADMIN),
                        physicalTableName);
            }
        }
    }

    
    public void handleRequireAccessOnDependentTable(String request, String userName, TableName dependentTable,
            String requestTable, Set<Action> requireAccess, Set<Action> accessExists) throws IOException {

        Set<Action> unionSet = new HashSet<Action>();
        unionSet.addAll(requireAccess);
        unionSet.addAll(accessExists);
        AUDITLOG.info(request + ": Automatically granting access to index table during creation of view:"
                + requestTable + authString(userName, dependentTable, requireAccess));
        grantPermissions(userName, dependentTable.getName(), unionSet.toArray(new Action[0]));
    }
    
    private void grantPermissions(final String toUser, final byte[] table, final Action... actions) throws IOException {
        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try (Connection conn = ConnectionFactory.createConnection(env.getConfiguration())) {
                    AccessControlClient.grant(conn, TableName.valueOf(table), toUser , null, null,
                            actions);
                } catch (Throwable e) {
                    new DoNotRetryIOException(e);
                }
                return null;
            }
        });
    }

    private void authorizeOrGrantAccessToUsers(final String request, final TableName fromTable,
            final List<Action> requiredActionsOnTable, final TableName toTable)
            throws IOException {
        User.runAsLoginUser(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
                try (Connection conn = ConnectionFactory.createConnection(env.getConfiguration())) {
                    List<UserPermission> userPermissions = getUserPermissions(fromTable.getNameAsString());
                    List<UserPermission> permissionsOnTheTable = getUserPermissions(toTable.getNameAsString());
                    if (userPermissions != null) {
                        for (UserPermission userPermission : userPermissions) {
                            Set<Action> requireAccess = new HashSet<Action>();
                            Set<Action> accessExists = new HashSet<Action>();
                            List<UserPermission> permsToTable = getPermissionForUser(permissionsOnTheTable,
                                    userPermission.getUser());
                            for (Action action : requiredActionsOnTable) {
                                boolean haveAccess=false;
                                if (userPermission.implies(action)) {
                                    if (permsToTable == null) {
                                        requireAccess.add(action);
                                    } else {
                                        for (UserPermission permToTable : permsToTable) {
                                            if (permToTable.implies(action)) {
                                                haveAccess=true;
                                            }
                                        }
                                        if (!haveAccess) {
                                            requireAccess.add(action);
                                        }
                                    }
                                }
                            }
                            if (permsToTable != null) {
                                // Append access to already existing access for the user
                                for (UserPermission permToTable : permsToTable) {
                                    accessExists.addAll(Arrays.asList(permToTable.getActions()));
                                }
                            }
                            if (!requireAccess.isEmpty()) {
                                if(AuthUtil.isGroupPrincipal(Bytes.toString(userPermission.getUser()))){
                                    AUDITLOG.warn("Users of GROUP:" + Bytes.toString(userPermission.getUser())
                                            + " will not have following access " + requireAccess
                                            + " to the newly created index " + toTable
                                            + ", Automatic grant is not yet allowed on Groups");
                                    continue;
                                }
                                handleRequireAccessOnDependentTable(request, Bytes.toString(userPermission.getUser()),
                                        toTable, toTable.getNameAsString(), requireAccess, accessExists);
                            }
                        }
                    }
                }
                return null;
            }
        });
    }

    private List<UserPermission> getPermissionForUser(List<UserPermission> perms, byte[] user) {
        if (perms != null) {
            // get list of permissions for the user as multiple implementation of AccessControl coprocessors can give
            // permissions for same users
            List<UserPermission> permissions = new ArrayList<>();
            for (UserPermission p : perms) {
                if (Bytes.equals(p.getUser(),user)){
                     permissions.add(p);
                }
            }
            if (!permissions.isEmpty()){
               return permissions;
            }
        }
        return null;
    }

    @Override
    public void preDropTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            List<PTable> indexes) throws IOException {
        if (!accessCheckEnabled) { return; }

        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            if (tableType != PTableType.VIEW) {
                observer.preDeleteTable(new ObserverContext<MasterCoprocessorEnvironment>(), physicalTableName);
            }
            if (indexes != null) {
                for (PTable index : indexes) {
                    observer.preDeleteTable(new ObserverContext<MasterCoprocessorEnvironment>(),
                            TableName.valueOf(index.getPhysicalName().getBytes()));
                }
            }
        }
        //checking similar permission checked during the create of the view.
        if (tableType == PTableType.VIEW || tableType == PTableType.INDEX) {
            requireAccess("Drop "+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
        }
    }

    @Override
    public void preAlterTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType) throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            if (tableType != PTableType.VIEW) {
            observer.preModifyTable(new ObserverContext<MasterCoprocessorEnvironment>(), physicalTableName,
                    new HTableDescriptor(physicalTableName));
            }
        }
        if (tableType == PTableType.VIEW) {
            requireAccess("Alter "+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
        }
    }

    @Override
    public void preGetSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preListNamespaceDescriptors(new ObserverContext<MasterCoprocessorEnvironment>(),
                    Arrays.asList(NamespaceDescriptor.create(schemaName).build()));
        }
    }

    @Override
    public void preCreateSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preCreateNamespace(new ObserverContext<MasterCoprocessorEnvironment>(),
                    NamespaceDescriptor.create(schemaName).build());
        }
    }

    @Override
    public void preDropSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preDeleteNamespace(new ObserverContext<MasterCoprocessorEnvironment>(), schemaName);
        }
    }

    @Override
    public void preIndexUpdate(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String indexName, TableName physicalTableName, TableName parentPhysicalTableName, PIndexState newState)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (BaseMasterAndRegionObserver observer : getAccessControllers()) {
            observer.preModifyTable(new ObserverContext<MasterCoprocessorEnvironment>(), physicalTableName,
                    new HTableDescriptor(physicalTableName));
        }
        // Check for read access in case of rebuild
        if (newState == PIndexState.BUILDING) {
            requireAccess("Rebuild:", parentPhysicalTableName, Action.READ, Action.EXEC);
        }
    }

    private List<UserPermission> getUserPermissions(final String tableName) throws IOException {
        return User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
            @Override
            public List<UserPermission> run() throws Exception {
                final List<UserPermission> userPermissions = new ArrayList<UserPermission>();
                try (Connection connection = ConnectionFactory.createConnection(env.getConfiguration())) {
                    for (BaseMasterAndRegionObserver service : accessControllers) {
                        if (service.getClass().getName().equals(org.apache.hadoop.hbase.security.access.AccessController.class.getName())) {
                            userPermissions.addAll(AccessControlClient.getUserPermissions(connection, tableName));
                        } else {
                            AccessControlProtos.GetUserPermissionsRequest.Builder builder = AccessControlProtos.GetUserPermissionsRequest
                                    .newBuilder();
                            builder.setTableName(ProtobufUtil.toProtoTableName(TableName.valueOf(tableName)));
                            builder.setType(AccessControlProtos.Permission.Type.Table);
                            AccessControlProtos.GetUserPermissionsRequest request = builder.build();

                            PayloadCarryingRpcController controller = ((ClusterConnection)connection)
                                    .getRpcControllerFactory().newController();
                            ((AccessControlService.Interface)service).getUserPermissions(controller, request,
                                    new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
                                        @Override
                                        public void run(AccessControlProtos.GetUserPermissionsResponse message) {
                                            if (message != null) {
                                                for (AccessControlProtos.UserPermission perm : message
                                                        .getUserPermissionList()) {
                                                    userPermissions.add(ProtobufUtil.toUserPermission(perm));
                                                }
                                            }
                                        }
                                    });
                        }
                    }
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        throw (Exception) e;
                    } else if (e instanceof Error) {
                        throw (Error) e;
                    }
                    throw new Exception(e);
                }
                return userPermissions;
            }
        });
    }
    
    /**
     * Authorizes that the current user has all the given permissions for the
     * given table
     * @param tableName Table requested
     * @throws IOException if obtaining the current user fails
     * @throws AccessDeniedException if user has no authorization
     */
    private void requireAccess(String request, TableName tableName, Action... permissions) throws IOException {
        User user = getActiveUser();
        AuthResult result = null;
        List<Action> requiredAccess = new ArrayList<Action>();
        for (Action permission : permissions) {
            if (hasAccess(getUserPermissions(tableName.getNameAsString()), tableName, permission, user)) {
                result = AuthResult.allow(request, "Table permission granted", user, permission, tableName, null, null);
            } else {
                result = AuthResult.deny(request, "Insufficient permissions", user, permission, tableName, null, null);
                requiredAccess.add(permission);
            }
            logResult(result);
        }
        if (!requiredAccess.isEmpty()) {
            result = AuthResult.deny(request, "Insufficient permissions", user, requiredAccess.get(0), tableName, null,
                    null);
        }
        if (!result.isAllowed()) { throw new AccessDeniedException("Insufficient permissions "
                + authString(user.getName(), tableName, new HashSet<Permission.Action>(Arrays.asList(permissions)))); }
    }

    /**
     * Checks if the user has access to the table for the specified action.
     *
     * @param perms All table permissions
     * @param table tablename
     * @param action action for access is required
     * @return true if the user has access to the table for specified action, false otherwise
     */
    private boolean hasAccess(List<UserPermission> perms, TableName table, Permission.Action action, User user) {
        if (Superusers.isSuperUser(user)){
            return true;
        }
        if (perms != null) {
            List<UserPermission> permissionsForUser = getPermissionForUser(perms, user.getShortName().getBytes());
            if (permissionsForUser != null) {
                for (UserPermission permissionForUser : permissionsForUser) {
                    if (permissionForUser.implies(action)) { return true; }
                }
            }
            String[] groupNames = user.getGroupNames();
            if (groupNames != null) {
              for (String group : groupNames) {
                List<UserPermission> groupPerms = getPermissionForUser(perms,(AuthUtil.toGroupEntry(group)).getBytes());
                if (groupPerms != null) for (UserPermission permissionForUser : groupPerms) {
                    if (permissionForUser.implies(action)) { return true; }
                }
              }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("No permissions found for table=" + table);
        }
        return false;
    }

    private User getActiveUser() throws IOException {
        User user = RpcServer.getRequestUser();
        if (user == null) {
            // for non-rpc handling, fallback to system user
            user = userProvider.getCurrent();
        }
        return user;
    }

    private void logResult(AuthResult result) {
        if (AUDITLOG.isTraceEnabled()) {
            InetAddress remoteAddr = RpcServer.getRemoteAddress();
            AUDITLOG.trace("Access " + (result.isAllowed() ? "allowed" : "denied") + " for user "
                    + (result.getUser() != null ? result.getUser().getShortName() : "UNKNOWN") + "; reason: "
                    + result.getReason() + "; remote address: " + (remoteAddr != null ? remoteAddr : "") + "; request: "
                    + result.getRequest() + "; context: " + result.toContextString());
        }
    }

    private static final class Superusers {
        private static final Log LOG = LogFactory.getLog(Superusers.class);

        /** Configuration key for superusers */
        public static final String SUPERUSER_CONF_KEY = org.apache.hadoop.hbase.security.Superusers.SUPERUSER_CONF_KEY; // Not getting a name

        private static List<String> superUsers;
        private static List<String> superGroups;
        private static User systemUser;

        private Superusers(){}

        /**
         * Should be called only once to pre-load list of super users and super
         * groups from Configuration. This operation is idempotent.
         * @param conf configuration to load users from
         * @throws IOException if unable to initialize lists of superusers or super groups
         * @throws IllegalStateException if current user is null
         */
        public static void initialize(Configuration conf) throws IOException {
            superUsers = new ArrayList<>();
            superGroups = new ArrayList<>();
            systemUser = User.getCurrent();

            if (systemUser == null) {
                throw new IllegalStateException("Unable to obtain the current user, "
                    + "authorization checks for internal operations will not work correctly!");
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace("Current user name is " + systemUser.getShortName());
            }
            String currentUser = systemUser.getShortName();
            String[] superUserList = conf.getStrings(SUPERUSER_CONF_KEY, new String[0]);
            for (String name : superUserList) {
                if (AuthUtil.isGroupPrincipal(name)) {
                    superGroups.add(AuthUtil.getGroupName(name));
                } else {
                    superUsers.add(name);
                }
            }
            superUsers.add(currentUser);
        }

        /**
         * @return true if current user is a super user (whether as user running process,
         * declared as individual superuser or member of supergroup), false otherwise.
         * @param user to check
         * @throws IllegalStateException if lists of superusers/super groups
         *   haven't been initialized properly
         */
        public static boolean isSuperUser(User user) {
            if (superUsers == null) {
                throw new IllegalStateException("Super users/super groups lists"
                    + " haven't been initialized properly.");
            }
            if (superUsers.contains(user.getShortName())) {
                return true;
            }

            for (String group : user.getGroupNames()) {
                if (superGroups.contains(group)) {
                    return true;
                }
            }
            return false;
        }

        public static List<String> getSuperUsers() {
            return superUsers;
        }

        public static User getSystemUser() {
            return systemUser;
        }
    }
    
    public String authString(String user, TableName table, Set<Action> actions) {
        StringBuilder sb = new StringBuilder();
        sb.append(" (user=").append(user != null ? user : "UNKNOWN").append(", ");
        sb.append("scope=").append(table == null ? "GLOBAL" : table.getNameWithNamespaceInclAsString()).append(", ");
        sb.append(actions.size() > 1 ? "actions=" : "action=").append(actions != null ? actions.toString() : "")
                .append(")");
        return sb.toString();
    }

}
