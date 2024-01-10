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
package org.apache.phoenix.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DefaultPhoenixMultiViewListProvider implements PhoenixMultiViewListProvider {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DefaultPhoenixMultiViewListProvider.class);

    public List<ViewInfoWritable> getPhoenixMultiViewList(Configuration configuration) {
        boolean isFetchAll = configuration.get(
                PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_ALL_VIEWS) != null;

        if (!isFetchAll) {
            return getTenantOrViewMultiViewList(configuration);
        }
        List<ViewInfoWritable> viewInfoWritables = new ArrayList<>();
        boolean isQueryMore = true;
        String query = PhoenixMultiInputUtil.getFetchViewQuery(configuration);

        int limit = PhoenixConfigurationUtil.getMultiViewQueryMoreSplitSize(configuration);

        String schema = null;
        String tableName = configuration.get(
                PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW);
        if (tableName != null) {
            schema = SchemaUtil.getSchemaNameFromFullName(tableName);
        }
        String tenantId = configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID);

        try (PhoenixConnection connection = (PhoenixConnection)
                ConnectionUtil.getInputConnection(configuration)){
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                do {
                    stmt.setString(1, tenantId);
                    stmt.setString(2, schema);
                    stmt.setString(3, tableName);
                    stmt.setInt(4, limit);

                    ResultSet viewRs = stmt.executeQuery();
                    String fullTableName = null;

                    while (viewRs.next()) {
                        tenantId = viewRs.getString(1);
                        schema = viewRs.getString(2);
                        tableName = viewRs.getString(3);
                        fullTableName = tableName;
                        Long viewTtlValue = viewRs.getLong(4);

                        if (schema != null && schema.length() > 0) {
                            fullTableName = SchemaUtil.getTableName(schema, tableName);
                        }

                        if (!isParentHasTTL(connection, tenantId, fullTableName)) {
                            addingViewIndexToTheFinalList(connection,tenantId,fullTableName,
                                    viewTtlValue, viewInfoWritables);
                        }
                    }
                    if (isQueryMore) {
                        if (fullTableName == null) {
                            isQueryMore = false;
                        }
                    }
                } while (isQueryMore);
            }

        }  catch (Exception e) {
            LOGGER.error("Getting view info failed with: " + e.getMessage());
        }
        return viewInfoWritables;
    }

    public List<ViewInfoWritable> getTenantOrViewMultiViewList(Configuration configuration) {
        List<ViewInfoWritable> viewInfoWritables = new ArrayList<>();
        String query = PhoenixMultiInputUtil.getFetchViewQuery(configuration);

        try (PhoenixConnection connection = (PhoenixConnection)
                ConnectionUtil.getInputConnection(configuration)) {
            try (Statement stmt = connection.createStatement()) {
                ResultSet viewRs = stmt.executeQuery(query);
                while (viewRs.next()) {
                    String tenantId = viewRs.getString(1);
                    String schema = viewRs.getString(2);
                    String tableName = viewRs.getString(3);
                    Long viewTtlValue = viewRs.getLong(4);
                    String fullTableName = tableName;

                    if (schema != null && schema.length() > 0) {
                        fullTableName = SchemaUtil.getTableName(schema, tableName);
                    }

                    if (!isParentHasTTL(connection, tenantId, fullTableName)) {
                        addingViewIndexToTheFinalList(connection,tenantId,fullTableName,
                                viewTtlValue, viewInfoWritables);
                    }
                }
            }
        }catch (Exception e) {
            LOGGER.error("Getting view info failed with: " + e.getMessage());
        }
        return viewInfoWritables;
    }

    private boolean isParentHasTTL(PhoenixConnection connection,
                                   String tenantId, String fullTableName) {
        boolean skip= false;
        try {
            PTable pTable = connection.getTable(tenantId, fullTableName);
            System.out.println("PTable");
            PTable parentTable = connection.getTable(null, pTable.getParentName().toString());
            System.out.println("Parent Table");
            if (parentTable.getType() == PTableType.VIEW &&
                    parentTable.getPhoenixTTL() > 0) {
                            /* if the current view parent already has a TTL value, we want to
                            skip the current view cleanup job because we want to run the cleanup
                             job for at the GlobalView level instead of running multi-jobs at
                             the LeafView level for the better performance.

                                     BaseTable
                               GlobalView(has TTL)
                            LeafView1, LeafView2, LeafView3....
                            */
                skip = true;
            }
        } catch (Exception e) {
            skip = true;
            LOGGER.error(String.format("Had an issue to process the view: %s, " +
                            "tenantId: see error %s ", fullTableName, tenantId,
                    e.getMessage()));
        }
        return skip;
    }

    private void addingViewIndexToTheFinalList(PhoenixConnection connection, String tenantId,
                                               String fullTableName, long viewTtlValue,
                                               List<ViewInfoWritable> viewInfoWritables)
            throws Exception {
        PTable pTable = connection.getTable(tenantId, fullTableName);
        ViewInfoWritable viewInfoTracker = new ViewInfoTracker(
                tenantId,
                fullTableName,
                viewTtlValue,
                pTable.getPhysicalName().getString(),
                false

        );
        viewInfoWritables.add(viewInfoTracker);

        List<PTable> allIndexesOnView = pTable.getIndexes();
        for (PTable viewIndexTable : allIndexesOnView) {
            String indexName = viewIndexTable.getTableName().getString();
            String indexSchema = viewIndexTable.getSchemaName().getString();
            if (indexName.contains(
                    QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR)) {
                indexName = SchemaUtil.getTableNameFromFullName(indexName,
                        QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR);
            }
            indexName = SchemaUtil.getTableNameFromFullName(indexName);
            indexName = SchemaUtil.getTableName(indexSchema, indexName);
            ViewInfoWritable viewInfoTrackerForIndexEntry = new ViewInfoTracker(
                    tenantId,
                    fullTableName,
                    viewTtlValue,
                    indexName,
                    true

            );
            viewInfoWritables.add(viewInfoTrackerForIndexEntry);
        }
    }
}