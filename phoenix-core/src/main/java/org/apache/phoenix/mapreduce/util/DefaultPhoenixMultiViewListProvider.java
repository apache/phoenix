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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class DefaultPhoenixMultiViewListProvider implements PhoenixMultiViewListProvider {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DefaultPhoenixMultiViewListProvider.class);

    public List<ViewInfoWritable> getPhoenixMultiViewList(Configuration configuration) {
        List<ViewInfoWritable> viewInfoWritables = new ArrayList<>();

        String query = PhoenixMultiInputUtil.getFetchViewQuery(configuration);
        boolean isQueryMore = configuration.get(
                PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_ALL_VIEWS) != null;
        int limit = PhoenixConfigurationUtil.getMultiViewQueryMoreSplitSize(configuration);
        try (PhoenixConnection connection = (PhoenixConnection)
                ConnectionUtil.getInputConnection(configuration)){
            try (Statement stmt = connection.createStatement()) {
                do {
                    ResultSet viewRs = stmt.executeQuery(query);
                    String schema = null;
                    String tableName = null;
                    String tenantId = null;
                    String fullTableName = null;

                    while (viewRs.next()) {
                        schema = viewRs.getString(2);
                        tableName = viewRs.getString(3);
                        tenantId = viewRs.getString(1);
                        fullTableName = tableName;
                        Long viewTtlValue = viewRs.getLong(4);

                        if (schema != null && schema.length() > 0) {
                            fullTableName = SchemaUtil.getTableName(schema, tableName);
                        }

                        boolean skip = false;
                        PTable pTable = null;
                        try {
                            pTable = PhoenixRuntime.getTable(connection, tenantId, fullTableName);
                            // we currently only support up to three levels
                            // CASE 1 : BASE_TABLE -> GLOBAL_VIEW -> TENANT_VIEW
                            // CASE 2 : BASE_TABLE -> TENANT_VIEW
                            // CASE 2 : BASE_TABLE -> VIEW
                            PTable parentTable = PhoenixRuntime.getTable(connection, null,
                                    pTable.getParentName().toString());
                            if (parentTable.getType() == PTableType.VIEW &&
                                    parentTable.getPhoenixTTL() > 0) {
                                skip = true;
                            }
                        } catch (Exception e) {
                            skip = true;
                            LOGGER.error(String.format("Had an issue to process the view: %s, tenantId:" +
                                    "see error %s ", fullTableName, tenantId, e.getMessage()));
                        }

                        if (!skip) {
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
                                if (indexName.contains(QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR)) {
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
                    if (isQueryMore) {
                        if (fullTableName == null) {
                            isQueryMore = false;
                        } else {
                            query = PhoenixMultiInputUtil.constructQueryMoreQuery(tenantId, schema, tableName, limit);
                        }
                    }
                } while (isQueryMore);
            }

        }  catch (Exception e) {
            LOGGER.error("Getting view info failed with: " + e.getMessage());
        }
        return viewInfoWritables;
    }
}