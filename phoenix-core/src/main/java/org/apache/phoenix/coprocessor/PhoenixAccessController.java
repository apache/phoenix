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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.ObserverContextImpl;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcUtil;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.AccessControlUtil;
import org.apache.hadoop.hbase.security.access.AuthResult;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost.PhoenixMetaDataControllerEnvironment;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class PhoenixAccessController extends BaseMetaDataEndpointObserver {

    private PhoenixMetaDataControllerEnvironment env;
    AtomicReference<ArrayList<MasterObserver>> accessControllers = new AtomicReference<>();
    private boolean hbaseAccessControllerEnabled;
    private boolean accessCheckEnabled;
    private boolean execPermissionsCheckEnabled;
    private UserProvider userProvider;
    private AccessChecker accessChecker;
    private Connection serverConnection;
    public static final Logger LOGGER = LoggerFactory.getLogger(PhoenixAccessController.class);
    private static final Logger AUDITLOG =
            LoggerFactory.getLogger("SecurityLogger."+PhoenixAccessController.class.getName());
    
    @Override
    public Optional<MetaDataEndpointObserver> getPhoenixObserver() {
        return Optional.of(this);
    }
    
    private List<MasterObserver> getAccessControllers() throws IOException {
        ArrayList<MasterObserver> oldAccessControllers = accessControllers.get();
        if (oldAccessControllers == null) {
            oldAccessControllers = new ArrayList<>();
            RegionCoprocessorHost cpHost = this.env.getCoprocessorHost();
            for (RegionCoprocessor cp : cpHost.findCoprocessors(RegionCoprocessor.class)) {
                if (cp instanceof AccessControlService.Interface && cp instanceof MasterObserver) {
                    oldAccessControllers.add((MasterObserver)cp);
                    if (cp.getClass().getName().equals(
                            org.apache.hadoop.hbase.security.access.AccessController.class.getName())) {
                        hbaseAccessControllerEnabled = true;
                    }
                }
            }
            accessControllers.set(oldAccessControllers);
        }
        return accessControllers.get();
    }

    public ObserverContext<MasterCoprocessorEnvironment> getMasterObsevrverContext() throws IOException {
        return new ObserverContextImpl<MasterCoprocessorEnvironment>(getActiveUser());
    }
    
    @Override
    public void preGetTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName) throws IOException {
        if (!accessCheckEnabled) { return; }
        if (this.execPermissionsCheckEnabled) {
            requireAccess("GetTable" + tenantId, physicalTableName, Action.READ, Action.EXEC);
        } else {
            requireAccess("GetTable" + tenantId, physicalTableName, Action.READ);
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        Configuration conf = env.getConfiguration();
        this.accessCheckEnabled = conf.getBoolean(QueryServices.PHOENIX_ACLS_ENABLED,
                QueryServicesOptions.DEFAULT_PHOENIX_ACLS_ENABLED);
        if (this.accessCheckEnabled) {
            //We cannot use try-with-resources with this, as the connection would get closed, but remain
            //in the cache, every invocation after the first would get a closed conn.
            serverConnection = ServerUtil.ConnectionFactory.getConnection(
                ServerUtil.ConnectionType.DEFAULT_SERVER_CONNECTION,
                ((PhoenixMetaDataControllerEnvironment) env).getRegionCoprocessorEnvironment());
        } else {
            LOGGER.warn(
                    "PhoenixAccessController has been loaded with authorization checks disabled.");
        }
        this.execPermissionsCheckEnabled = conf.getBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY,
                AccessControlConstants.DEFAULT_EXEC_PERMISSION_CHECKS);
        if (env instanceof PhoenixMetaDataControllerEnvironment) {
            this.env = (PhoenixMetaDataControllerEnvironment)env;
        } else {
            throw new IllegalArgumentException(
                    "Not a valid environment, should be loaded by PhoenixMetaDataControllerEnvironment");
        }

        accessChecker = new AccessChecker(env.getConfiguration());
        // set the user-provider.
        this.userProvider = UserProvider.instantiate(env.getConfiguration());
        // init superusers and add the server principal (if using security)
        // or process owner as default super user.
        Superusers.initialize(env.getConfiguration());
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        super.stop(env);
    }

    @Override
    public void preCreateTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType,
            Set<byte[]> familySet, Set<TableName> indexes) throws IOException {
        if (!accessCheckEnabled) { return; }
        
        if (tableType != PTableType.VIEW) {
            TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(physicalTableName);
            for (byte[] familyName : familySet) {
                tableDescBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(familyName).build());
            }
            final TableDescriptor htd = tableDescBuilder.build();
            for (MasterObserver observer : getAccessControllers()) {
                observer.preCreateTable(getMasterObsevrverContext(), htd, null);
            }
        }

        // Index and view require read access on parent physical table.
        Set<TableName> physicalTablesChecked = new HashSet<TableName>();
        if (tableType == PTableType.VIEW || tableType == PTableType.INDEX) {
            physicalTablesChecked.add(parentPhysicalTableName);
            if (execPermissionsCheckEnabled) {
                requireAccess("Create" + tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
            } else {
                requireAccess("Create" + tableType, parentPhysicalTableName, Action.READ);
            }
        }

        if (tableType == PTableType.VIEW) {

            Action[] requiredActions = execPermissionsCheckEnabled ?
                    new Action[]{ Action.READ, Action.EXEC } : new Action[] { Action.READ};
            for (TableName index : indexes) {
                if (!physicalTablesChecked.add(index)) {
                    // skip check for local index as we have already check the ACLs above
                    // And for same physical table multiple times like view index table
                    continue;
                }

                User user = getActiveUser();
                List<UserPermission> permissionForUser = getPermissionForUser(
                        getUserPermissions(index), user.getShortName());
                Set<Action> requireAccess = new HashSet<>();
                Set<Action> accessExists = new HashSet<>();
                if (permissionForUser != null) {
                    for (UserPermission userPermission : permissionForUser) {
                        for (Action action : Arrays.asList(requiredActions)) {
                            if (!userPermission.getPermission().implies(action)) {
                                requireAccess.add(action);
                            }
                        }
                    }
                    if (!requireAccess.isEmpty()) {
                        for (UserPermission userPermission : permissionForUser) {
                            accessExists.addAll(Arrays.asList(
                                userPermission.getPermission().getActions()));
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
                List<Action> actions = new ArrayList<>(Arrays.asList(Action.READ, Action.WRITE, Action.CREATE, Action.ADMIN));
                if (execPermissionsCheckEnabled) {
                    actions.add(Action.EXEC);
                }
                authorizeOrGrantAccessToUsers("Create" + tableType, parentPhysicalTableName,
                        actions, physicalTableName);
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
                try {
                    AccessControlClient.grant(serverConnection, TableName.valueOf(table), toUser , null, null,
                            actions);
                } catch (Throwable e) {
                    throw new DoNotRetryIOException(e);
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
                List<UserPermission> userPermissions = getUserPermissions(fromTable);
                List<UserPermission> permissionsOnTheTable = getUserPermissions(toTable);
                if (userPermissions != null) {
                    for (UserPermission userPermission : userPermissions) {
                        Set<Action> requireAccess = new HashSet<Action>();
                        Set<Action> accessExists = new HashSet<Action>();
                        List<UserPermission> permsToTable = getPermissionForUser(permissionsOnTheTable,
                                userPermission.getUser());
                        for (Action action : requiredActionsOnTable) {
                            boolean haveAccess=false;
                            if (userPermission.getPermission().implies(action)) {
                                if (permsToTable == null) {
                                    requireAccess.add(action);
                                } else {
                                    for (UserPermission permToTable : permsToTable) {
                                        if (permToTable.getPermission().implies(action)) {
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
                                accessExists.addAll(Arrays.asList(
                                    permToTable.getPermission().getActions()));
                            }
                        }
                        if (!requireAccess.isEmpty()) {
                            if (AuthUtil.isGroupPrincipal(userPermission.getUser())){
                                AUDITLOG.warn("Users of GROUP:" + userPermission.getUser()
                                        + " will not have following access " + requireAccess
                                        + " to the newly created index " + toTable
                                        + ", Automatic grant is not yet allowed on Groups");
                                continue;
                            }
                            handleRequireAccessOnDependentTable(request,
                                    userPermission.getUser(), toTable,
                                    toTable.getNameAsString(), requireAccess, accessExists);
                        }
                    }
                }
                return null;
            }
        });
    }

    private List<UserPermission> getPermissionForUser(List<UserPermission> perms, String user) {
        if (perms != null) {
            // get list of permissions for the user as multiple implementation of AccessControl coprocessors can give
            // permissions for same users
            List<UserPermission> permissions = new ArrayList<>();
            for (UserPermission p : perms) {
                if (p.getUser().equals(user)){
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

        for (MasterObserver observer : getAccessControllers()) {
            if (tableType != PTableType.VIEW) {
                observer.preDeleteTable(getMasterObsevrverContext(), physicalTableName);
            }
            if (indexes != null) {
                for (PTable index : indexes) {
                    observer.preDeleteTable(getMasterObsevrverContext(),
                            TableName.valueOf(index.getPhysicalName().getBytes()));
                }
            }
        }
        //checking similar permission checked during the create of the view.
        if (tableType == PTableType.VIEW || tableType == PTableType.INDEX) {
            if (execPermissionsCheckEnabled) {
                requireAccess("Drop "+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
            } else {
                requireAccess("Drop "+tableType, parentPhysicalTableName, Action.READ);
            }
        }
    }

    @Override
    public void preAlterTable(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String tableName, TableName physicalTableName, TableName parentPhysicalTableName, PTableType tableType) throws IOException {
        if (!accessCheckEnabled) { return; }
        for (MasterObserver observer : getAccessControllers()) {
            if (tableType != PTableType.VIEW) {
            observer.preModifyTable(getMasterObsevrverContext(), physicalTableName, null,
                    TableDescriptorBuilder.newBuilder(physicalTableName).build());
            }
        }
        if (tableType == PTableType.VIEW) {
            if (execPermissionsCheckEnabled) {
                requireAccess("Alter "+tableType, parentPhysicalTableName, Action.READ, Action.EXEC);
            } else {
                requireAccess("Alter "+tableType, parentPhysicalTableName, Action.READ);
            }
        }
    }

    @Override
    public void preGetSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (MasterObserver observer : getAccessControllers()) {
            observer.preListNamespaceDescriptors(getMasterObsevrverContext(),
                    Arrays.asList(NamespaceDescriptor.create(schemaName).build()));
        }
    }

    @Override
    public void preCreateSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (MasterObserver observer : getAccessControllers()) {
            observer.preCreateNamespace(getMasterObsevrverContext(),
                    NamespaceDescriptor.create(schemaName).build());
        }
    }

    @Override
    public void preDropSchema(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String schemaName)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (MasterObserver observer : getAccessControllers()) {
            observer.preDeleteNamespace(getMasterObsevrverContext(), schemaName);
        }
    }

    @Override
    public void preIndexUpdate(ObserverContext<PhoenixMetaDataControllerEnvironment> ctx, String tenantId,
            String indexName, TableName physicalTableName, TableName parentPhysicalTableName, PIndexState newState)
            throws IOException {
        if (!accessCheckEnabled) { return; }
        for (MasterObserver observer : getAccessControllers()) {
            observer.preModifyTable(getMasterObsevrverContext(), physicalTableName, null,
                    TableDescriptorBuilder.newBuilder(physicalTableName).build());
        }
        // Check for read access in case of rebuild
        if (newState == PIndexState.BUILDING) {
            if (execPermissionsCheckEnabled) {
                requireAccess("Rebuild:", parentPhysicalTableName, Action.READ, Action.EXEC);
            } else {
                requireAccess("Rebuild:", parentPhysicalTableName, Action.READ);
            }
        }
    }

    /**
      * Gets all the permissions for a given tableName for all the users
      * Also, get the permissions at table's namespace level and merge all of them
      * @throws IOException
      */
     private List<UserPermission> getUserPermissions(final TableName tableName) throws IOException {
         List<UserPermission> userPermissions =
                 User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
            @Override
            public List<UserPermission> run() throws Exception {
                final List<UserPermission> userPermissions = new ArrayList<UserPermission>();
                final RpcCall rpcContext = RpcUtil.getRpcContext();
                try {
                    // Setting RPC context as null so that user can be resetted
                    RpcUtil.setRpcContext(null);
                    // Merge permissions from all accessController coprocessors loaded in memory
                    for (MasterObserver service : getAccessControllers()) {
                        // Use AccessControlClient API's if the accessController is an instance of org.apache.hadoop.hbase.security.access.AccessController
                        if (service.getClass().getName().equals(
                                org.apache.hadoop.hbase.security.access.AccessController.class.getName())) {
                            userPermissions.addAll(AccessControlClient.getUserPermissions(
                                    serverConnection, tableName.getNameWithNamespaceInclAsString()));
                            userPermissions.addAll(AccessControlClient.getUserPermissions(
                                    serverConnection, AuthUtil.toGroupEntry(tableName.getNamespaceAsString())));
                        }
                    }
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        throw (Exception) e;
                    } else if (e instanceof Error) {
                        throw (Error) e;
                    }
                    throw new Exception(e);
                } finally {
                    // Setting RPC context back to original context of the RPC
                    RpcUtil.setRpcContext(rpcContext);
                }
                return userPermissions;
            }
         });
         getUserDefinedPermissions(tableName, userPermissions);
         return userPermissions;
       }

     private void getUserDefinedPermissions(final TableName tableName,
             final List<UserPermission> userPermissions) throws IOException {
          User.runAsLoginUser(new PrivilegedExceptionAction<List<UserPermission>>() {
              @Override
              public List<UserPermission> run() throws Exception {
                  final RpcCall rpcContext = RpcUtil.getRpcContext();
                  try {
                      // Setting RPC context as null so that user can be resetted
                      RpcUtil.setRpcContext(null);
                      for (MasterObserver service : getAccessControllers()) {
                         if (service.getClass().getName().equals(
                             org.apache.hadoop.hbase.security.access.AccessController.class
                                     .getName())) {
                              continue;
                           } else {
                             getUserPermsFromUserDefinedAccessController(userPermissions,
                                 (AccessControlService.Interface) service);
                           }
                      }
                  } catch (Throwable e) {
                      if (e instanceof Exception) {
                          throw (Exception) e;
                      } else if (e instanceof Error) {
                          throw (Error) e;
                      }
                      throw new Exception(e);
                  } finally {
                      // Setting RPC context back to original context of the RPC
                      RpcUtil.setRpcContext(rpcContext);
                  }
                  return userPermissions;
              }
            private void getUserPermsFromUserDefinedAccessController(final List<UserPermission> userPermissions, AccessControlService.Interface service) {

                RpcController controller = new ServerRpcController();

                AccessControlProtos.GetUserPermissionsRequest.Builder builderTablePerms = AccessControlProtos.GetUserPermissionsRequest
                        .newBuilder();
                builderTablePerms.setTableName(ProtobufUtil.toProtoTableName(tableName));
                builderTablePerms.setType(AccessControlProtos.Permission.Type.Table);
                AccessControlProtos.GetUserPermissionsRequest requestTablePerms = builderTablePerms.build();

                callGetUserPermissionsRequest(userPermissions, service, requestTablePerms, controller);

                AccessControlProtos.GetUserPermissionsRequest.Builder builderNamespacePerms = AccessControlProtos.GetUserPermissionsRequest
                        .newBuilder();
                builderNamespacePerms.setNamespaceName(ByteString.copyFrom(tableName.getNamespace()));
                builderNamespacePerms.setType(AccessControlProtos.Permission.Type.Namespace);
                AccessControlProtos.GetUserPermissionsRequest requestNamespacePerms = builderNamespacePerms.build();

                callGetUserPermissionsRequest(userPermissions, service, requestNamespacePerms, controller);

            }

            private void callGetUserPermissionsRequest(final List<UserPermission> userPermissions, AccessControlService.Interface service
                    , AccessControlProtos.GetUserPermissionsRequest request, RpcController controller) {
                service.getUserPermissions(controller, request,
                    new RpcCallback<AccessControlProtos.GetUserPermissionsResponse>() {
                        @Override
                        public void run(AccessControlProtos.GetUserPermissionsResponse message) {
                            if (message != null) {
                                for (AccessControlProtos.UserPermission perm : message
                                        .getUserPermissionList()) {
                                    userPermissions.add(AccessControlUtil.toUserPermission(perm));
                                }
                            }
                        }
                    });
            }
        });
    }

    /**
     * Authorizes that the current user has all the given permissions for the
     * given table and for the hbase namespace of the table
     * @param tableName Table requested
     * @throws IOException if obtaining the current user fails
     * @throws AccessDeniedException if user has no authorization
     */
    private void requireAccess(String request, TableName tableName, Action... permissions) throws IOException {
        User user = getActiveUser();
        AuthResult result = null;
        List<Action> requiredAccess = new ArrayList<Action>();

        for (Action permission : permissions) {
             if (hasAccess(getUserPermissions(tableName), tableName, permission, user)) {
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
     * @param perms All table and table's namespace permissions
     * @param table tablename
     * @param action action for access is required
     * @return true if the user has access to the table for specified action, false otherwise
     */
    private boolean hasAccess(List<UserPermission> perms, TableName table, Permission.Action action, User user) {
        if (Superusers.isSuperUser(user)){
            return true;
        }
        if (perms != null) {
            if (hbaseAccessControllerEnabled
                    && accessChecker.getAuthManager().authorizeUserTable(user, table, action)) {
                return true;
            }
            List<UserPermission> permissionsForUser =
                    getPermissionForUser(perms, user.getShortName());
            if (permissionsForUser != null) {
                for (UserPermission permissionForUser : permissionsForUser) {
                    if (permissionForUser.getPermission().implies(action)) { return true; }
                }
            }
            String[] groupNames = user.getGroupNames();
            if (groupNames != null) {
              for (String group : groupNames) {
                List<UserPermission> groupPerms =
                        getPermissionForUser(perms, (AuthUtil.toGroupEntry(group)));
                if (groupPerms != null) for (UserPermission permissionForUser : groupPerms) {
                    if (permissionForUser.getPermission().implies(action)) { return true; }
                }
              }
            }
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("No permissions found for table=" +
                    table + " or namespace=" + table.getNamespaceAsString());
        }
        return false;
    }

    private User getActiveUser() throws IOException {
        Optional<User> user = RpcServer.getRequestUser();
        if (!user.isPresent()) {
            // for non-rpc handling, fallback to system user
            return userProvider.getCurrent();
        }
        return user.get();
    }

    private void logResult(AuthResult result) {
        if (AUDITLOG.isTraceEnabled()) {
            Optional<InetAddress> remoteAddr = RpcServer.getRemoteAddress();
            AUDITLOG.trace("Access " + (result.isAllowed() ? "allowed" : "denied") + " for user "
                    + (result.getUser() != null ? result.getUser().getShortName() : "UNKNOWN") + "; reason: "
                    + result.getReason() + "; remote address: " + (remoteAddr.isPresent() ? remoteAddr.get() : "") + "; request: "
                    + result.getRequest() + "; context: " + result.toContextString());
        }
    }

    private static final class Superusers {
        private static final Logger LOGGER = LoggerFactory.getLogger(Superusers.class);

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

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Current user name is " + systemUser.getShortName());
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

    }

    public String authString(String user, TableName table, Set<Action> actions) {
        StringBuilder sb = new StringBuilder();
        sb.append(" (user=").append(user != null ? user : "UNKNOWN").append(", ");
        sb.append("scope=").append(table == null ? "GLOBAL" : table.getNameWithNamespaceInclAsString()).append(", ");
        sb.append(actions.size() > 1 ? "actions=" : "action=").append(actions.toString())
                .append(")");
        return sb.toString();
    }

}
