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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerViewUtil extends ViewUtil {
    private static final Logger logger = LoggerFactory.getLogger(ServerViewUtil.class);

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
                    PTable.LinkType.CHILD_TABLE,
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
                    view = connection.getTableNoCache(SchemaUtil
                            .getTableName(viewSchemaName, viewName));
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
