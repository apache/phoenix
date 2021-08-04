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

import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Gets index column names and their data table equivalents
 */
public class IndexColumnNames {
    private List<String> dataNonPkColNames = Lists.newArrayList();
    private List<String> dataPkColNames = Lists.newArrayList();
    private List<String> dataColNames;
    protected List<String> dataColSqlTypeNames = Lists.newArrayList();
    private List<String> indexPkColNames = Lists.newArrayList();
    private List<String> indexNonPkColNames = Lists.newArrayList();
    private List<String> indexColNames;
    protected List<String> indexColSqlTypeNames = Lists.newArrayList();
    private PTable pdataTable;
    private PTable pindexTable;

    public IndexColumnNames(final PTable pdataTable, final PTable pindexTable) {
        this.pdataTable = pdataTable;
        this.pindexTable = pindexTable;
        List<PColumn> pindexCols = pindexTable.getColumns();
        List<PColumn> pkColumns = pindexTable.getPKColumns();
        Set<String> indexColsAdded = new HashSet<String>();
        int offset = 0;
        if (pindexTable.getBucketNum() != null) {
            offset++;
        }
        if (pindexTable.getViewIndexId() != null) {
            offset++;
        }
        if (pindexTable.isMultiTenant() && pindexTable.getViewIndexId() != null) {
            offset++;
        }

        if (offset > 0) {
            pindexCols = pindexCols.subList(offset, pindexCols.size());
            pkColumns = pkColumns.subList(offset, pkColumns.size());
        }

        // first add the data pk columns
        for (PColumn indexCol : pindexCols) {
            if (IndexUtil.isDataPKColumn(indexCol)) {
                String indexColumnName = indexCol.getName().getString();
                PColumn dPkCol = IndexUtil.getDataColumn(pdataTable, indexColumnName);
                dataPkColNames.add(getDataColFullName(dPkCol));
                dataColSqlTypeNames.add(getDataTypeString(dPkCol));
                indexPkColNames.add(indexColumnName);
                indexColSqlTypeNames.add(getDataTypeString(indexCol));
                indexColsAdded.add(indexColumnName);
            }
        }

        // then the rest of the index pk columns
        for (PColumn indexPkCol : pkColumns) {
            String indexColName = indexPkCol.getName().getString();
            if (!indexColsAdded.contains(indexColName)) {
                indexPkColNames.add(indexColName);
                indexColSqlTypeNames.add(getDataTypeString(indexPkCol));
                PColumn dCol = IndexUtil.getDataColumn(pdataTable, indexColName);
                dataNonPkColNames.add(getDataColFullName(dCol));
                dataColSqlTypeNames.add(getDataTypeString(dCol));
                indexColsAdded.add(indexColName);
            }
        }

        // then the covered columns (rest of the columns)
        for (PColumn indexCol : pindexCols) {
            String indexColName = indexCol.getName().getString();
            if (!indexColsAdded.contains(indexColName)) {
                indexNonPkColNames.add(indexColName);
                indexColSqlTypeNames.add(getDataTypeString(indexCol));
                PColumn dCol = IndexUtil.getDataColumn(pdataTable, indexColName);
                dataNonPkColNames.add(getDataColFullName(dCol));
                dataColSqlTypeNames.add(getDataTypeString(dCol));
            }
        }

        indexColNames = Lists.newArrayList(Iterables.concat(indexPkColNames, indexNonPkColNames));
        dataColNames = Lists.newArrayList(Iterables.concat(dataPkColNames, dataNonPkColNames));
    }

    private String getDataTypeString(PColumn col) {
        PDataType<?> dataType = col.getDataType();
        switch (dataType.getSqlType()) {
        case Types.DECIMAL:
            String typeStr = dataType.toString();
            if (col.getMaxLength() != null) {
                typeStr += "(" + col.getMaxLength().toString();
                if (col.getScale() != null) {
                    typeStr += "," + col.getScale().toString();
                }
                typeStr += ")";
            }
            return typeStr;
        default:
            if (col.getMaxLength() != null) {
                return String.format("%s(%s)", dataType.toString(), col.getMaxLength());
            }
            return dataType.toString();
        }
    }

    public static String getDataColFullName(PColumn dCol) {
        String dColFullName = "";
        if (dCol.getFamilyName() != null) {
            dColFullName += dCol.getFamilyName().getString() + QueryConstants.NAME_SEPARATOR;
        }
        dColFullName += dCol.getName().getString();
        return dColFullName;
    }

    private List<String> getDynamicCols(List<String> colNames, List<String> colTypes) {
        List<String> dynamicCols = Lists.newArrayListWithCapacity(colNames.size());
        for (int i = 0; i < colNames.size(); i++) {
            String dataColName = colNames.get(i);
            String dataColType = colTypes.get(i);
            String dynamicCol =
                    SchemaUtil.getEscapedFullColumnName(dataColName) + " " + dataColType;
            dynamicCols.add(dynamicCol);
        }
        return dynamicCols;
    }

    private List<String> getUnqualifiedColNames(List<String> qualifiedCols) {
        return Lists.transform(qualifiedCols, new Function<String, String>() {
            @Override
            public String apply(String qCol) {
                return SchemaUtil.getTableNameFromFullName(qCol, QueryConstants.NAME_SEPARATOR);
            }
        });
    }

    protected List<String> getCastedColumnNames(List<String> colNames, List<String> castTypes) {
        List<String> castColNames = Lists.newArrayListWithCapacity(colNames.size());
        colNames = SchemaUtil.getEscapedFullColumnNames(colNames);
        for (int i = 0; i < colNames.size(); i++) {
            castColNames.add("CAST(" + colNames.get(i) + " AS " + castTypes.get(i) + ")");
        }
        return castColNames;
    }

    public String getQualifiedDataTableName() {
        return SchemaUtil.getQualifiedTableName(pdataTable.getSchemaName().getString(),
            pdataTable.getTableName().getString());
    }

    public String getQualifiedIndexTableName() {
        return SchemaUtil.getQualifiedTableName(pindexTable.getSchemaName().getString(),
            pindexTable.getTableName().getString());
    }

    /**
     * @return the escaped data column names (equivalents for the index columns) along with their
     *         sql type, for use in dynamic column queries/upserts
     */
    public List<String> getDynamicDataCols() {
        // don't want the column family for dynamic columns
        return getDynamicCols(getUnqualifiedDataColNames(), dataColSqlTypeNames);

    }

    /**
     * @return the escaped index column names along with their sql type, for use in dynamic column
     *         queries/upserts
     */
    public List<String> getDynamicIndexCols() {
        // don't want the column family for dynamic columns
        return getDynamicCols(getUnqualifiedIndexColNames(), indexColSqlTypeNames);
    }

    /**
     * @return the corresponding data table column names for the index columns, leading with the
     *         data table pk columns
     */
    public List<String> getDataColNames() {
        return dataColNames;
    }

    /**
     * @return same as getDataColNames, without the column family qualifier
     */
    public List<String> getUnqualifiedDataColNames() {
        return getUnqualifiedColNames(dataColNames);
    }

    /**
     * @return the corresponding data table column names for the index columns, which are not part
     *         of the data table pk
     */
    public List<String> getDataNonPkColNames() {
        return dataNonPkColNames;
    }

    /**
     * @return the corresponding data table column names for the index columns, which are part of
     *         the data table pk
     */
    public List<String> getDataPkColNames() {
        return dataPkColNames;
    }

    /**
     * @return the index column names, leading with the data table pk columns
     */
    public List<String> getIndexColNames() {
        return indexColNames;
    }

    /**
     * @return same as getIndexColNames, without the column family qualifier
     */
    public List<String> getUnqualifiedIndexColNames() {
        return getUnqualifiedColNames(indexColNames);
    }

    /**
     * @return the index pk column names
     */
    public List<String> getIndexPkColNames() {
        return indexPkColNames;
    }
}
