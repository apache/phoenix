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
package org.apache.phoenix.tools.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;

import java.sql.Connection;
import java.sql.ResultSet;

public class PhckRowCounterCheckTool {
    private Configuration conf;

    public PhckRowCounterCheckTool() {

    }

    public PhckRowCounterCheckTool(Configuration conf) {
        this.conf = conf;
    }

    public String getTableInfoQuery(String tenantId, String schema, String tableName) {
        String query = String.format("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME = '%s'", tableName);

        if (tenantId == null) {
            query = query + " AND TENANT_ID IS NULL";
        } else {
            query = query + String.format( " AND TENANT_ID = '%s'", tenantId);
        }

        if (schema == null) {
            query = query + " AND TABLE_SCHEM IS NULL";
        } else {
            query = query + String.format( " AND TABLE_SCHEM = '%s'", schema);
        }

        return query;
    }

    public PhckTable getPhckTable(String tenantId, String schema, String tableName) throws Exception {
        PhckTable phckTable = new PhckTable();
        try (Connection conn = ConnectionUtil.getInputConnection(conf)){
            ResultSet rs = conn.createStatement().executeQuery(
                    getTableInfoQuery(tenantId, schema, tableName));

            while (rs.next()) {
                PhckRow row = new PhckRow(rs, PhckUtil.PHCK_ROW_RESOURCE.CATALOG);

                if (row.isHeadRow()) {
                    phckTable.setPhckHeadRow(row);
                } else {
                    phckTable.addPhckColumnRows(row);
                }
            }
        }

        return phckTable;
    }
}
