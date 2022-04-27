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

import java.sql.ResultSet;

public class PhckRow {
    boolean isHeadRow;
    boolean isSyscatRow;
    int headRowColumnRowCount;

    private String tenantId;
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String columnFamily;

    private String tableType;

    private String columnCount;
    private String linkType;
    private String indexState;
    private String indexType;
    private String viewType;
    private String qualifierCounter;
    private String ordinalPosition;
    private String columnType;

    private PhckUtil.PHCK_ROW_RESOURCE phckRowResource;

    PhckRow() {

    }

    public PhckRow(ResultSet resultSet, PhckUtil.PHCK_ROW_RESOURCE phckRowResource)
            throws Exception {
        this.phckRowResource = phckRowResource;
        this.tenantId = resultSet.getString(1);
        this.tableSchema = resultSet.getString(2);
        this.tableName = resultSet.getString(3);
        this.columnName = resultSet.getString(4);
        this.columnFamily = resultSet.getString(5);

        if (this.phckRowResource == PhckUtil.PHCK_ROW_RESOURCE.CATALOG) {
            this.isSyscatRow = true;
            this.linkType = resultSet.getString(33);
            this.columnCount = resultSet.getString(9);
            this.tableType = resultSet.getString(7);
            if (this.tableType != null && this.tableType.length() > 0) {
                this.isHeadRow = true;
                try {
                    this.headRowColumnRowCount = Integer.valueOf(this.columnCount);
                } catch (Exception e) {
                    this.headRowColumnRowCount = -1;
                }
            }
        } else {
            this.linkType = resultSet.getString(6);
        }
    }

    public boolean isHeadRow() {
        return isHeadRow;
    }

    public int getHeadRowColumnRowCount() {
        return this.headRowColumnRowCount;
    }


}
