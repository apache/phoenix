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
package org.apache.phoenix.mapreduce.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ServerBuildTransformingTableCompiler;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixServerBuildIndexInputFormat;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.getIndexToolIndexTableName;

public class PhoenixTransformWithViewsInputFormat<T extends DBWritable> extends PhoenixServerBuildIndexInputFormat {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixTransformWithViewsInputFormat.class);
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        try (PhoenixConnection connection = (PhoenixConnection)
                ConnectionUtil.getInputConnection(configuration)) {
            try (Table hTable = connection.unwrap(PhoenixConnection.class).getQueryServices().getTable(
                    SchemaUtil.getPhysicalTableName(SYSTEM_CHILD_LINK_NAME_BYTES, configuration).toBytes())) {
                String oldDataTableFullName = PhoenixConfigurationUtil.getIndexToolDataTableName(configuration);
                String newDataTableFullName = getIndexToolIndexTableName(configuration);
                PTable newDataTable = connection.getTableNoCache(newDataTableFullName);
                String schemaName = SchemaUtil.getSchemaNameFromFullName(oldDataTableFullName);
                String tableName = SchemaUtil.getTableNameFromFullName(oldDataTableFullName);
                byte[] schemaNameBytes = Strings.isNullOrEmpty(schemaName) ? null : schemaName.getBytes();
                Pair<List<PTable>, List<TableInfo>> allDescendantViews = ViewUtil.findAllDescendantViews(hTable, configuration, null, schemaNameBytes,
                        tableName.getBytes(), EnvironmentEdgeManager.currentTimeMillis(), false);
                List<PTable> legitimateDecendants = allDescendantViews.getFirst();

                List<InputSplit> inputSplits = new ArrayList<>();

                HashMap<String, PColumn> columnMap = new HashMap<>();
                for (PColumn column : newDataTable.getColumns()) {
                    columnMap.put(column.getName().getString(), column);
                }

                for (PTable decendant : legitimateDecendants) {
                    if (decendant.getViewType() == PTable.ViewType.READ_ONLY) {
                        continue;
                    }
                    PTable newView = Transform.getTransformedView(decendant, newDataTable, columnMap, true);
                    QueryPlan queryPlan = getQueryPlan(newView, decendant, connection);
                    inputSplits.addAll(generateSplits(queryPlan, configuration));
                }
                if (inputSplits.size() == 0) {
                    // Get for base table
                    ServerBuildTransformingTableCompiler compiler = new ServerBuildTransformingTableCompiler(connection,
                            oldDataTableFullName);
                    MutationPlan plan = compiler.compile(newDataTable);
                    inputSplits.addAll(generateSplits(plan.getQueryPlan(), configuration));
                }
                return inputSplits;
            }
        } catch (Exception e) {
            LOGGER.error("PhoenixTransformWithViewsInputFormat failed with: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private QueryPlan getQueryPlan(PTable newTable, PTable oldTable, PhoenixConnection phoenixConnection) throws SQLException {
        String tableTenantId = oldTable.getTenantId() == null? null:oldTable.getTenantId().getString();
        String connTenantId = phoenixConnection.getTenantId()==null? null:phoenixConnection.getTenantId().getString();
        if (!Strings.isNullOrEmpty(tableTenantId) && !StringUtils.equals(tableTenantId, connTenantId)) {
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tableTenantId);

            try (PhoenixConnection tenantConnection = (PhoenixConnection)
                    DriverManager.getConnection(phoenixConnection.getURL(), props)) {
                return getQueryPlanInternal(newTable, oldTable, tenantConnection);
            }
        }
        return getQueryPlanInternal(newTable, oldTable, phoenixConnection);
    }

    private QueryPlan getQueryPlanInternal(PTable newTable, PTable decendant, PhoenixConnection phoenixConnection) throws SQLException {
        ServerBuildTransformingTableCompiler compiler = new ServerBuildTransformingTableCompiler(phoenixConnection,
                SchemaUtil.getTableName(decendant.getSchemaName(), decendant.getTableName()).getString());

        MutationPlan plan = compiler.compile(newTable);
        return plan.getQueryPlan();
    }
}
