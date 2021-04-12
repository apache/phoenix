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
package org.apache.phoenix.util;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PARENT_TENANT_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_BYTES;
import static org.apache.phoenix.util.PhoenixRuntime.CURRENT_SCN_ATTRIB;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.TableInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerViewUtil extends ViewUtil {

    private static final Logger logger = LoggerFactory.getLogger(ServerViewUtil.class);

    
    /**
     * Find all the descendant views of a given table or view in a depth-first fashion.
     * Note that apart from scanning the {@code parent->child } links, we also validate each view
     * by trying to resolve it.
     * Use {@link ViewUtil#findAllRelatives(Table, byte[], byte[], byte[], LinkType,
     * TableViewFinderResult)} if you want to find other links and don't care about orphan results.
     *
     * @param sysCatOrsysChildLink Table corresponding to either SYSTEM.CATALOG or SYSTEM.CHILD_LINK
     * @param serverSideConfig server-side configuration
     * @param tenantId tenantId of the view (null if it is a table or global view)
     * @param schemaName schema name of the table/view
     * @param tableOrViewName name of the table/view
     * @param clientTimeStamp client timestamp
     * @param findJustOneLegitimateChildView if true, we are only interested in knowing if there is
     *                                       at least one legitimate child view, so we return early.
     *                                       If false, we want to find all legitimate child views
     *                                       and all orphan views (views that no longer exist)
     *                                       stemming from this table/view and all of its legitimate
     *                                       child views.
     *
     * @return a Pair where the first element is a list of all legitimate child views (or just 1
     * child view in case findJustOneLegitimateChildView is true) and where the second element is
     * a list of all orphan views stemming from this table/view and all of its legitimate child
     * views (in case findJustOneLegitimateChildView is true, this list will be incomplete since we
     * are not interested in it anyhow)
     *
     * @throws IOException thrown if there is an error scanning SYSTEM.CHILD_LINK or SYSTEM.CATALOG
     * @throws SQLException thrown if there is an error getting a connection to the server or an
     * error retrieving the PTable for a child view
     */
    public static Pair<List<PTable>, List<TableInfo>> findAllDescendantViews(
            Table sysCatOrsysChildLink, Configuration serverSideConfig, byte[] tenantId,
            byte[] schemaName, byte[] tableOrViewName, long clientTimeStamp,
            boolean findJustOneLegitimateChildView)
            throws IOException, SQLException {
        List<PTable> legitimateChildViews = new ArrayList<>();
        List<TableInfo> orphanChildViews = new ArrayList<>();

        findAllDescendantViews(sysCatOrsysChildLink, serverSideConfig, tenantId, schemaName,
                tableOrViewName, clientTimeStamp, legitimateChildViews, orphanChildViews,
                findJustOneLegitimateChildView);
        return new Pair<>(legitimateChildViews, orphanChildViews);
    }

    private static void findAllDescendantViews(Table sysCatOrsysChildLink,
            Configuration serverSideConfig, byte[] parentTenantId, byte[] parentSchemaName,
            byte[] parentTableOrViewName, long clientTimeStamp, List<PTable> legitimateChildViews,
            List<TableInfo> orphanChildViews, boolean findJustOneLegitimateChildView)
            throws IOException, SQLException {
        TableViewFinderResult currentResult =
                findImmediateRelatedViews(sysCatOrsysChildLink, parentTenantId, parentSchemaName,
                        parentTableOrViewName, LinkType.CHILD_TABLE, clientTimeStamp);
        for (TableInfo viewInfo : currentResult.getLinks()) {
            byte[] viewTenantId = viewInfo.getTenantId();
            byte[] viewSchemaName = viewInfo.getSchemaName();
            byte[] viewName = viewInfo.getTableName();
            PTable view;
            Properties props = new Properties();
            if (viewTenantId != null) {
                props.setProperty(TENANT_ID_ATTRIB, Bytes.toString(viewTenantId));
            }
            if (clientTimeStamp != HConstants.LATEST_TIMESTAMP) {
                props.setProperty(CURRENT_SCN_ATTRIB, Long.toString(clientTimeStamp));
            }
            try (PhoenixConnection connection =
                    QueryUtil.getConnectionOnServer(props, serverSideConfig)
                            .unwrap(PhoenixConnection.class)) {
                try {
                    view = PhoenixRuntime.getTableNoCache(connection,
                            SchemaUtil.getTableName(viewSchemaName, viewName));
                } catch (TableNotFoundException ex) {
                    logger.error("Found an orphan parent->child link keyed by this parent."
                            + " Parent Tenant Id: '" + Bytes.toString(parentTenantId)
                            + "'. Parent Schema Name: '" + Bytes.toString(parentSchemaName)
                            + "'. Parent Table/View Name: '" + Bytes.toString(parentTableOrViewName)
                            + "'. The child view which could not be resolved has ViewInfo: '"
                            + viewInfo + "'.", ex);
                    orphanChildViews.add(viewInfo);
                    // Prune orphan branches
                    continue;
                }

                if (isLegitimateChildView(view, parentSchemaName, parentTableOrViewName)) {
                    legitimateChildViews.add(view);
                    // return early since we're only interested in knowing if there is at least one
                    // valid child view
                    if (findJustOneLegitimateChildView) {
                        break;
                    }
                    // Note that we only explore this branch if the current view is a legitimate
                    // child view, else we ignore it and move on to the next potential child view
                    findAllDescendantViews(sysCatOrsysChildLink, serverSideConfig,
                            viewInfo.getTenantId(), viewInfo.getSchemaName(),
                            viewInfo.getTableName(), clientTimeStamp, legitimateChildViews,
                            orphanChildViews, findJustOneLegitimateChildView);
                } else {
                    logger.error("Found an orphan parent->child link keyed by this parent."
                            + " Parent Tenant Id: '" + Bytes.toString(parentTenantId)
                            + "'. Parent Schema Name: '" + Bytes.toString(parentSchemaName)
                            + "'. Parent Table/View Name: '" + Bytes.toString(parentTableOrViewName)
                            + "'. There currently exists a legitimate view of the same name which"
                            + " is not a descendant of this table/view. View Info: '" + viewInfo
                            + "'. Ignoring this view and not counting it as a child view.");
                    // Prune unrelated view branches left around due to orphan parent->child links
                }
            }
        }
    }



    
    /**
     * Attempt to drop an orphan child view i.e. a child view for which we see a
     * {@code parent->child } entry
     * in SYSTEM.CHILD_LINK/SYSTEM.CATALOG (as a child) but for whom the parent no longer exists.
     * @param env Region Coprocessor environment
     * @param tenantIdBytes tenantId of the parent
     * @param schemaName schema of the parent
     * @param tableOrViewName parent table/view name
     * @param sysCatOrSysChildLink SYSTEM.CATALOG or SYSTEM.CHILD_LINK which is used to find the
     *                             {@code parent->child } linking rows
     * @throws IOException thrown if there is an error scanning SYSTEM.CHILD_LINK or SYSTEM.CATALOG
     * @throws SQLException thrown if there is an error getting a connection to the server or
     * an error retrieving the PTable for a child view
     */
    public static void dropChildViews(RegionCoprocessorEnvironment env, byte[] tenantIdBytes,
            byte[] schemaName, byte[] tableOrViewName, byte[] sysCatOrSysChildLink)
            throws IOException, SQLException {
        Table hTable = null;
        try {
            hTable = ServerUtil.getHTableForCoprocessorScan(env, SchemaUtil.getPhysicalTableName(
                            sysCatOrSysChildLink, env.getConfiguration()));
        } catch (Exception e){
            logger.error("ServerUtil.getHTableForCoprocessorScan error!", e);
        }
        // if the SYSTEM.CATALOG or SYSTEM.CHILD_LINK doesn't exist just return
        if (hTable==null) {
            return;
        }

        TableViewFinderResult childViewsResult;
        try {
            childViewsResult = findImmediateRelatedViews(
                    hTable,
                    tenantIdBytes,
                    schemaName,
                    tableOrViewName,
                    LinkType.CHILD_TABLE,
                    HConstants.LATEST_TIMESTAMP);
        } finally {
            hTable.close();
        }

        for (TableInfo viewInfo : childViewsResult.getLinks()) {
            byte[] viewTenantId = viewInfo.getTenantId();
            byte[] viewSchemaName = viewInfo.getSchemaName();
            byte[] viewName = viewInfo.getTableName();
            if (logger.isDebugEnabled()) {
                logger.debug("dropChildViews : " + Bytes.toString(schemaName) + "."
                        + Bytes.toString(tableOrViewName) + " -> "
                        + Bytes.toString(viewSchemaName) + "." + Bytes.toString(viewName)
                        + "with tenant id :" + Bytes.toString(viewTenantId));
            }
            Properties props = new Properties();
            PTable view = null;
            if (viewTenantId != null && viewTenantId.length != 0)
                props.setProperty(TENANT_ID_ATTRIB, Bytes.toString(viewTenantId));
            try (PhoenixConnection connection = QueryUtil.getConnectionOnServer(props,
                    env.getConfiguration()).unwrap(PhoenixConnection.class)) {
                try {
                    // Ensure that the view to be dropped has some ancestor that no longer exists
                    // (and thus will throw a TableNotFoundException). Otherwise, if we are looking
                    // at an orphan parent->child link, then the view might actually be a legitimate
                    // child view on another table/view and we should obviously not drop it
                    view = PhoenixRuntime.getTableNoCache(connection,
                            SchemaUtil.getTableName(viewSchemaName, viewName));
                } catch (TableNotFoundException expected) {
                    // Expected for an orphan view since some ancestor was dropped earlier
                    logger.info("Found an expected orphan parent->child link keyed by the parent."
                            + " Parent Tenant Id: '" + Bytes.toString(tenantIdBytes)
                            + "'. Parent Schema Name: '" + Bytes.toString(schemaName)
                            + "'. Parent Table/View Name: '" + Bytes.toString(tableOrViewName)
                            + "'. Will attempt to drop this child view with ViewInfo: '"
                            + viewInfo + "'.");
                }
                if (view != null) {
                    logger.error("Found an orphan parent->child link keyed by this parent or"
                            + " its descendant. Parent Tenant Id: '" + Bytes.toString(tenantIdBytes)
                            + "'. Parent Schema Name: '" + Bytes.toString(schemaName)
                            + "'. Parent Table/View Name: '" + Bytes.toString(tableOrViewName)
                            + "'. There currently exists a legitimate view of the same name whose"
                            + " parent hierarchy exists. View Info: '" + viewInfo
                            + "'. Ignoring this view and not attempting to drop it.");
                    continue;
                }

                MetaDataClient client = new MetaDataClient(connection);
                org.apache.phoenix.parse.TableName viewTableName =
                        org.apache.phoenix.parse.TableName.create(Bytes.toString(viewSchemaName),
                                Bytes.toString(viewName));
                try {
                    client.dropTable(new DropTableStatement(viewTableName, PTableType.VIEW, true,
                            true, true));
                } catch (TableNotFoundException e) {
                    logger.info("Ignoring view " + viewTableName
                            + " as it has already been dropped");
                }
            }
        }
    }

}
