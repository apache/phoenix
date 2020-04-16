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
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class DefaultPhoenixMultiViewListProvider implements PhoenixMultiViewListProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPhoenixMultiViewListProvider.class);

    public List<ViewInfoWritable> getPhoenixMultiViewList(Configuration configuration) {
        List<ViewInfoWritable> viewInfoWritables = new ArrayList<>();
        String query = getFetchViewQuery(configuration);
        try (PhoenixConnection connection = (PhoenixConnection)
                ConnectionUtil.getInputConnection(configuration, new Properties())){
            TableName catalogOrChildTableName = ViewUtil.getSystemTableForChildLinks(0, configuration);
            Table catalogOrChildTable = connection.getQueryServices()
                    .getTable(SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                            connection.getQueryServices().getProps())
                            .getName());
            ResultSet viewRs = connection.createStatement().executeQuery(query);
            while (viewRs.next()) {
                String schema = viewRs.getString(2);
                String tableName = viewRs.getString(3);
                String tenantId = viewRs.getString(1);
                String fullTableName = tableName;
                Long viewTtlValue = viewRs.getLong(4);

                if (schema != null && schema.length() > 0) {
                    fullTableName = SchemaUtil.getTableName(schema, tableName);
                }

                byte[] tenantIdInBytes = tenantId == null ? new byte[0] : tenantId.getBytes(StandardCharsets.UTF_8);
                byte[] tableNameInBytes = tableName == null ? new byte[0] : tableName.getBytes(StandardCharsets.UTF_8);
                byte[] schemaInBytes = schema == null ? new byte[0] : schema.getBytes(StandardCharsets.UTF_8);

                boolean hasChildViews = ViewUtil.hasChildViews(catalogOrChildTable, tenantIdInBytes, schemaInBytes,
                        tableNameInBytes, System.currentTimeMillis());

                if (hasChildViews) {
                    LOGGER.debug("Skip intermediate view : " + fullTableName);
                } else {
                    // this will only apply for leaf view
                    ViewInfoWritable viewInfoTracker = new ViewInfoTracker(
                            tenantId,
                            fullTableName,
                            viewTtlValue
                    );
                    viewInfoWritables.add(viewInfoTracker);
                }
            }
        } catch (SQLException e ) {
            LOGGER.error("Getting view info failed with: " + e.getMessage());
        } catch (Exception e) {

        }

        return viewInfoWritables;
    }

    public static String getFetchViewQuery(Configuration configuration) {
        String query;
        if (configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_ALL_VIEWS) != null) {
            query = PhoenixViewTtlUtil.SELECT_ALL_VIEW_METADATA_FROM_SYSCAT_QUERY;
        } else if ((configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_PER_TABLE) != null)) {
            query = PhoenixViewTtlUtil.constructViewMetadataQueryBasedOnTable(
                    configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_PER_TABLE));
        } else if (configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID) != null &&
                configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_PER_VIEW) == null) {

            query = PhoenixViewTtlUtil.constructViewMetadataQueryBasedOnTenant(
                    configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID));

        } else {
            query = PhoenixViewTtlUtil.constructViewMetadataQueryBasedOnView(
                    configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_DELETE_JOB_PER_VIEW),
                    configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID));
        }

        return query;
    }
}