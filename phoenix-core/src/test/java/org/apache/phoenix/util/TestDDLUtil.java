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

import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

import java.sql.Connection;
import java.sql.SQLException;

public class TestDDLUtil {
    private boolean isNamespaceMapped;
    private boolean isChangeDetectionEnabled;

    public TestDDLUtil(boolean isNamespaceMapped) {
        this.isNamespaceMapped = isNamespaceMapped;
    }

    public void createBaseTable(Connection conn, String schemaName, String tableName,
                                 boolean multiTenant,
                                 Integer saltBuckets, String splits, boolean immutable)
        throws SQLException {
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String ddl = "CREATE " + (immutable ? "IMMUTABLE" : "") +
            " TABLE " + SchemaUtil.getTableName(schemaName, tableName) +
            " (t_id VARCHAR NOT NULL,\n" +
            "k1 VARCHAR NOT NULL,\n" +
            "k2 INTEGER NOT NULL,\n" +
            "v1 VARCHAR,\n" +
            "v2 INTEGER,\n" +
            "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n";
        String ddlOptions = multiTenant ? "MULTI_TENANT=true" : "";
        if (saltBuckets != null) {
            ddlOptions = ddlOptions
                + (ddlOptions.isEmpty() ? "" : ", ")
                + "salt_buckets=" + saltBuckets;
        }
        if (isChangeDetectionEnabled) {
            ddlOptions = ddlOptions + (ddlOptions.isEmpty() ? "" : ", ") +
                "CHANGE_DETECTION_ENABLED=TRUE";
        }
        if (splits != null) {
            ddlOptions = ddlOptions
                + (ddlOptions.isEmpty() ? "" : ", ")
                + "splits=" + splits;
        }
        conn.createStatement().execute(ddl + ddlOptions);
    }

    public void createIndex(Connection conn, String schemaName, String indexName,
                            String tableName, String indexedColumnName, boolean isLocal,
                            boolean isAsync) throws SQLException {
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String local = isLocal ? " LOCAL " : "";
        String async = isAsync ? " ASYNC " : "";
        String sql =
            "CREATE " + local + " INDEX " + indexName + " ON " + fullTableName + "(" +
        indexedColumnName + ")" + async;
        conn.createStatement().execute(sql);
    }
    public void createView(Connection conn, String schemaName, String viewName,
                        String baseTableName) throws SQLException {
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        String fullTableName = SchemaUtil.getTableName(schemaName, baseTableName);
        String viewSql = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName;
        if (isChangeDetectionEnabled) {
            viewSql = viewSql + " " + PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED + "=TRUE";
        }
        conn.createStatement().execute(viewSql);
    }

    public void createViewIndex(Connection conn, String schemaName, String indexName,
                               String viewName,
                                 String indexColumn) throws SQLException {
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullViewName + "(" + indexColumn + ")");
    }

    public void setChangeDetectionEnabled(boolean isChangeDetectionEnabled) {
        this.isChangeDetectionEnabled = isChangeDetectionEnabled;
    }
}
