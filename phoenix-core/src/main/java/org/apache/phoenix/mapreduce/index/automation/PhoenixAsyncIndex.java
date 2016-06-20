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
package org.apache.phoenix.mapreduce.index.automation;

import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.PTable.IndexType;

public class PhoenixAsyncIndex {
    private String tableName;
    private IndexType indexType;
    private String tableSchem;
    private String dataTableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public String getTableSchem() {
        return tableSchem;
    }

    public void setTableSchem(String tableSchem) {
        this.tableSchem = tableSchem;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public void setDataTableName(String dataTableName) {
        this.dataTableName = dataTableName;
    }

    public String getJobName() {
        return String.format(IndexTool.INDEX_JOB_NAME_TEMPLATE, dataTableName, tableName);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TableName = " + tableName)
                .append(" ; IndexType = " + (indexType == null ? "" : indexType.toString()))
                .append(" ; TableSchem = " + tableSchem)
                .append(" ; DataTableName = " + dataTableName);
        return builder.toString();
    }

}
