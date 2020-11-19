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
import org.apache.phoenix.mapreduce.PhoenixTTLTool;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PHOENIX_TTL_NOT_DEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_TYPE;

public class PhoenixMultiInputUtil {
    public static final String SELECT_ALL_VIEW_METADATA_FROM_SYSCAT_QUERY =
            "SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, PHOENIX_TTL FROM " +
                    SYSTEM_CATALOG_NAME + " WHERE " +
                    TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "' AND " +
                    PHOENIX_TTL + " IS NOT NULL AND " +
                    PHOENIX_TTL + " > " + PHOENIX_TTL_NOT_DEFINED + " AND " +
                    VIEW_TYPE + " <> " + PTable.ViewType.MAPPED.getSerializedValue();

    public static Connection buildTenantConnection(String url, String tenantId)
            throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    public static String getSelectAllPageQuery() {
        return SELECT_ALL_VIEW_METADATA_FROM_SYSCAT_QUERY + " AND " +
                "(TENANT_ID,TABLE_SCHEM,TABLE_NAME) > (?,?,?) LIMIT ?";
    }

    public static String constructViewMetadataQueryBasedOnView(String fullName, String tenantId) {
        String query = SELECT_ALL_VIEW_METADATA_FROM_SYSCAT_QUERY;


        if (fullName != null) {
            if (fullName.equals(PhoenixTTLTool.DELETE_ALL_VIEWS)) {
                return query;
            }

            String schema = SchemaUtil.getSchemaNameFromFullName(fullName);
            String viewName = SchemaUtil.getTableNameFromFullName(fullName);

            if (!schema.equals(StringUtil.EMPTY_STRING)) {
                query = query + " AND TABLE_SCHEM = '" + schema + "'";
            } else {
                query = query + " AND TABLE_SCHEM IS NULL";
            }

            query = query + " AND TABLE_NAME = '" + viewName + "'";
        }

        if (tenantId != null && tenantId.length() > 0) {
            query = query + " AND TENANT_ID = '" + tenantId + "'";
        } else {
            query = query + " AND TENANT_ID IS NULL";
        }

        return query;
    }


    public static String constructViewMetadataQueryBasedOnTenant(String tenant) {
        return constructViewMetadataQueryBasedOnView(null, tenant);
    }

    public static String getFetchViewQuery(Configuration configuration) {
        String query;
        if (configuration.get(
                PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_ALL_VIEWS) != null) {
            query = PhoenixMultiInputUtil.getSelectAllPageQuery();
        } else if (configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID) != null &&
                configuration.get(PhoenixConfigurationUtil.
                        MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW) == null) {
            query = PhoenixMultiInputUtil.constructViewMetadataQueryBasedOnTenant(
                    configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID));
        } else {
            query = PhoenixMultiInputUtil.constructViewMetadataQueryBasedOnView(
                    configuration.get(
                            PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW),
                    configuration.get(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID));
        }
        return query;
    }
}