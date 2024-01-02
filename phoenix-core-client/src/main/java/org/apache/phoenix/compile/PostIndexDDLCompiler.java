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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.StringUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import static org.apache.phoenix.util.IndexUtil.INDEX_COLUMN_NAME_SEP;


/**
 * Class that compiles plan to generate initial data values after a DDL command for
 * index table.
 */
public class PostIndexDDLCompiler {
    private final PhoenixConnection connection;
    private final TableRef dataTableRef;
    private List<String> indexColumnNames;
    private List<String> dataColumnNames;
    private String selectQuery;
    private boolean forTransform = false;

    public PostIndexDDLCompiler(PhoenixConnection connection, TableRef dataTableRef) {
        this.connection = connection;
        this.dataTableRef = dataTableRef;
        indexColumnNames = Lists.newArrayList();
        dataColumnNames = Lists.newArrayList();
    }

    public PostIndexDDLCompiler(PhoenixConnection connection, TableRef dataTableRef, boolean forTransform) {
        this(connection, dataTableRef);
        this.forTransform = forTransform;
    }

    public MutationPlan compile(final PTable indexTable) throws SQLException {
        /*
         * Compiles an UPSERT SELECT command to read from the data table and populate the index table
         */
        StringBuilder indexColumns = new StringBuilder();
        StringBuilder dataColumns = new StringBuilder();
        
        // Add the pk index columns
        List<PColumn> indexPKColumns = indexTable.getPKColumns();
        int nIndexPKColumns = indexTable.getPKColumns().size();
        boolean isSalted = indexTable.getBucketNum() != null;
        boolean isMultiTenant = connection.getTenantId() != null && indexTable.isMultiTenant();
        boolean isViewIndex = indexTable.getViewIndexId()!=null;
        int posOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isViewIndex ? 1 : 0);
        for (int i = posOffset; i < nIndexPKColumns; i++) {
            PColumn col = indexPKColumns.get(i);
            String indexColName = col.getName().getString();
            // need to escape backslash as this used in the SELECT statement
            String dataColName = col.getExpressionStr() == null ? col.getName().getString()
                    : StringUtil.escapeBackslash(col.getExpressionStr());
            dataColumns.append(dataColName).append(",");
            indexColumns.append('"').append(indexColName).append("\",");
            indexColumnNames.add(indexColName);
            dataColumnNames.add(dataColName);
        }
        
        // Add the covered columns
        for (PColumnFamily family : indexTable.getColumnFamilies()) {
            for (PColumn col : family.getColumns()) {
                if (col.getViewConstant() == null) {
                    String indexColName = col.getName().getString();
                    // Transforming tables also behave like indexes but they don't have index_col_name_sep. So we use family name directly.
                    String dataFamilyName = indexColName.indexOf(INDEX_COLUMN_NAME_SEP)!=-1 ? IndexUtil.getDataColumnFamilyName(indexColName) :
                            col.getFamilyName().getString();
                    String dataColumnName = IndexUtil.getDataColumnName(indexColName);
                    if (!dataFamilyName.equals("")) {
                        dataColumns.append('"').append(dataFamilyName).append("\".");
                        if (forTransform) {
                            // transforming table columns have the same family name
                            indexColumns.append('"').append(dataFamilyName).append("\".");
                        }
                    }
                    dataColumns.append('"').append(dataColumnName).append("\",");
                    indexColumns.append('"').append(indexColName).append("\",");
                    indexColumnNames.add(indexColName);
                    dataColumnNames.add(dataColumnName);
                }
            }
        }

        final PTable dataTable = dataTableRef.getTable();
        dataColumns.setLength(dataColumns.length()-1);
        indexColumns.setLength(indexColumns.length()-1);
        String schemaName = dataTable.getSchemaName().getString();
        String tableName = indexTable.getTableName().getString();
        
        StringBuilder updateStmtStr = new StringBuilder();
        updateStmtStr.append("UPSERT /*+ NO_INDEX */ INTO ").append(schemaName.length() == 0 ? "" : '"' + schemaName + "\".").append('"').append(tableName).append("\"(")
           .append(indexColumns).append(") ");
        final StringBuilder selectQueryBuilder = new StringBuilder();
        selectQueryBuilder.append(" SELECT /*+ NO_INDEX */ ").append(dataColumns).append(" FROM ")
        .append(schemaName.length() == 0 ? "" : '"' + schemaName + "\".").append('"').append(dataTable.getTableName().getString()).append('"');
        this.selectQuery = selectQueryBuilder.toString();
        updateStmtStr.append(this.selectQuery);
        
        try (final PhoenixStatement statement = new PhoenixStatement(connection)) {
            DelegateMutationPlan delegate = new DelegateMutationPlan(statement.compileMutation(updateStmtStr.toString())) {
                @Override
                public MutationState execute() throws SQLException {
                    connection.getMutationState().commitDDLFence(dataTable);
                    return super.execute();
                }
            };
            return delegate;
        }
    }

    public List<String> getIndexColumnNames() {
        return indexColumnNames;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    public String getSelectQuery() {
        return selectQuery;
    }

}
