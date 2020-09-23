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
package org.apache.phoenix.schema;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;


import static org.apache.phoenix.util.MetaDataUtil.SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES;

public class SchemaExtractionProcessor {
    Map<String, String> defaultProps = new HashMap<>();
    Map<String, String> definedProps = new HashMap<>();

    private static final String CREATE_TABLE = "CREATE TABLE %s";
    private static final String CREATE_INDEX = "CREATE %sINDEX %s ON %s";
    private static final String CREATE_VIEW = "CREATE VIEW %s%s AS SELECT * FROM %s%s";

    private PTable table;
    private Configuration conf;
    private String ddl = null;
    private String tenantId;

    public SchemaExtractionProcessor(String tenantId, Configuration conf,
            String pSchemaName, String pTableName)
            throws SQLException {
        this.tenantId = tenantId;
        this.conf = conf;
        this.table = getPTable(pSchemaName, pTableName);
    }

    public String process() throws Exception {
        if (ddl != null) {
            return ddl;
        }
        if(this.table.getType().equals(PTableType.TABLE)) {
            ddl = extractCreateTableDDL(this.table);
        } else if(this.table.getType().equals(PTableType.INDEX)) {
            ddl = extractCreateIndexDDL(this.table);
        } else if(this.table.getType().equals(PTableType.VIEW)) {
            ddl = extractCreateViewDDL(this.table);
        }
        return ddl;
    }

    protected String extractCreateIndexDDL(PTable indexPTable)
            throws SQLException {
        String pTableName = indexPTable.getTableName().getString();

        String baseTableName = indexPTable.getParentTableName().getString();
        String baseTableFullName = SchemaUtil
                .getQualifiedTableName(indexPTable.getSchemaName().getString(), baseTableName);
        PTable dataPTable = getPTable(baseTableFullName);

        String defaultCF = SchemaUtil.getEmptyColumnFamilyAsString(indexPTable);
        String indexedColumnsString = getIndexedColumnsString(indexPTable, dataPTable, defaultCF);
        String coveredColumnsString = getCoveredColumnsString(indexPTable, defaultCF);

        return generateIndexDDLString(baseTableFullName, indexedColumnsString, coveredColumnsString,
                indexPTable.getIndexType().equals(PTable.IndexType.LOCAL), pTableName);
    }

    //TODO: Indexed on an expression
    //TODO: test with different CF
    private String getIndexedColumnsString(PTable indexPTable, PTable dataPTable, String defaultCF) {

        List<PColumn> indexPK = indexPTable.getPKColumns();
        List<PColumn> dataPK = dataPTable.getPKColumns();
        Set<String> indexPkSet = new HashSet<>();
        Set<String> dataPkSet = new HashSet<>();
        Map<String, SortOrder> sortOrderMap = new HashMap<>();
        StringBuilder indexedColumnsBuilder = new StringBuilder();
        for (PColumn indexedColumn : indexPK) {
            String indexColumn = extractIndexColumn(indexedColumn.getName().getString(), defaultCF);
            if(indexColumn.equalsIgnoreCase(MetaDataUtil.VIEW_INDEX_ID_COLUMN_NAME)) {
                continue;
            }
            indexPkSet.add(indexColumn);
            sortOrderMap.put(indexColumn, indexedColumn.getSortOrder());
        }

        for(PColumn pColumn : dataPK) {
            dataPkSet.add(pColumn.getName().getString());
        }

        Set<String> effectivePK = Sets.symmetricDifference(indexPkSet, dataPkSet);
        if (effectivePK.isEmpty()) {
            effectivePK = indexPkSet;
        }
        for (String column : effectivePK) {
            if(indexedColumnsBuilder.length()!=0) {
                indexedColumnsBuilder.append(", ");
            }
            indexedColumnsBuilder.append(column);
            if(sortOrderMap.containsKey(column) && sortOrderMap.get(column) != SortOrder.getDefault()) {
                indexedColumnsBuilder.append(" ");
                indexedColumnsBuilder.append(sortOrderMap.get(column));
            }
        }
        return indexedColumnsBuilder.toString();
    }

    private String extractIndexColumn(String columnName, String defaultCF) {
        String [] columnNameSplit = columnName.split(":");
        if(columnNameSplit[0].equals("") || columnNameSplit[0].equalsIgnoreCase(defaultCF)) {
            return columnNameSplit[1];
        } else {
            return columnNameSplit.length > 1 ?
                    String.format("\"%s\".\"%s\"", columnNameSplit[0], columnNameSplit[1]) : columnNameSplit[0];
        }
    }

    private String getCoveredColumnsString(PTable indexPTable, String defaultCF) {
        StringBuilder coveredColumnsBuilder = new StringBuilder();
        List<PColumn> pkColumns = indexPTable.getColumns();
        for (PColumn cc : pkColumns) {
            if(coveredColumnsBuilder.length()!=0) {
                coveredColumnsBuilder.append(", ");
            }
            if(cc.getFamilyName()!=null) {
                String indexColumn = extractIndexColumn(cc.getName().getString(), defaultCF);
                coveredColumnsBuilder.append(indexColumn);
            }
        }
        return coveredColumnsBuilder.toString();
    }

    protected String generateIndexDDLString(String baseTableFullName, String indexedColumnString,
            String coveredColumnString, boolean local, String pTableName) {
        StringBuilder outputBuilder = new StringBuilder(String.format(CREATE_INDEX,
                local ? "LOCAL " : "", pTableName, baseTableFullName));
        outputBuilder.append("(");
        outputBuilder.append(indexedColumnString);
        outputBuilder.append(")");
        if(!coveredColumnString.equals("")) {
            outputBuilder.append(" INCLUDE (");
            outputBuilder.append(coveredColumnString);
            outputBuilder.append(")");
        }
        return outputBuilder.toString();
    }

    PTable getPTable(String pTableFullName) throws SQLException {
        try (Connection conn = getConnection()) {
            return PhoenixRuntime.getTable(conn, pTableFullName);
        }
    }

    protected String extractCreateViewDDL(PTable table) throws SQLException {
        String pSchemaName = table.getSchemaName().getString();
        String pTableName = table.getTableName().getString();
        String baseTableName = table.getParentTableName().getString();
        String baseTableFullName = SchemaUtil.getQualifiedTableName(pSchemaName, baseTableName);
        PTable baseTable = getPTable(baseTableFullName);
        String columnInfoString = getColumnInfoStringForView(table, baseTable);

        String whereClause = table.getViewStatement();
        if(whereClause != null) {
            whereClause = whereClause.substring(whereClause.indexOf("WHERE"));
        }
        return generateCreateViewDDL(columnInfoString, baseTableFullName,
                whereClause == null ? "" : " "+whereClause, pSchemaName, pTableName);
    }

    private String generateCreateViewDDL(String columnInfoString, String baseTableFullName,
            String whereClause, String pSchemaName, String pTableName) {
        String viewFullName = SchemaUtil.getPTableFullNameWithQuotes(pSchemaName, pTableName);
        StringBuilder outputBuilder = new StringBuilder(String.format(CREATE_VIEW, viewFullName,
                columnInfoString, baseTableFullName, whereClause));
        return outputBuilder.toString();
    }

    public String extractCreateTableDDL(PTable table) throws IOException, SQLException {
        String pSchemaName = table.getSchemaName().getString();
        String pTableName = table.getTableName().getString();

        ConnectionQueryServices cqsi = getCQSIObject();
        HTableDescriptor htd = getTableDescriptor(cqsi, table);
        HColumnDescriptor[] hcds = htd.getColumnFamilies();
        populateDefaultProperties(table);
        setPTableProperties(table);
        setHTableProperties(htd);
        setHColumnFamilyProperties(hcds);

        String columnInfoString = getColumnInfoStringForTable(table);
        String propertiesString = convertPropertiesToString();

        return generateTableDDLString(columnInfoString, propertiesString, pSchemaName, pTableName);
    }

    private String generateTableDDLString(String columnInfoString, String propertiesString,
            String pSchemaName, String pTableName) {
        String pTableFullName = SchemaUtil.getPTableFullNameWithQuotes(pSchemaName, pTableName);
        StringBuilder outputBuilder = new StringBuilder(String.format(CREATE_TABLE, pTableFullName));
        outputBuilder.append(columnInfoString).append(" ").append(propertiesString);
        return outputBuilder.toString();
    }

    private void populateDefaultProperties(PTable table) {
        Map<String, String> propsMap = HColumnDescriptor.getDefaultValues();
        for (Map.Entry<String, String> entry : propsMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            defaultProps.put(key, value);
            if(key.equalsIgnoreCase(HColumnDescriptor.BLOOMFILTER) || key.equalsIgnoreCase(
                    HColumnDescriptor.COMPRESSION)) {
                defaultProps.put(key, "NONE");
            }
            if(key.equalsIgnoreCase(HColumnDescriptor.DATA_BLOCK_ENCODING)) {
                defaultProps.put(key, String.valueOf(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING));
            }
        }
        defaultProps.putAll(table.getDefaultPropertyValues());
    }

    private void setHTableProperties(HTableDescriptor htd) {
        Map<ImmutableBytesWritable, ImmutableBytesWritable> propsMap = htd.getValues();
        for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry : propsMap.entrySet()) {
            ImmutableBytesWritable key = entry.getKey();
            ImmutableBytesWritable value = entry.getValue();
            if(Bytes.toString(key.get()).contains("coprocessor") || Bytes.toString(key.get()).contains(
                    HTableDescriptor.IS_META)) {
                continue;
            }
            defaultProps.put(Bytes.toString(key.get()), "false");
            definedProps.put(Bytes.toString(key.get()), Bytes.toString(value.get()));
        }
    }

    private void setHColumnFamilyProperties(HColumnDescriptor[] columnDescriptors) {
        Map<ImmutableBytesWritable, ImmutableBytesWritable> propsMap = columnDescriptors[0].getValues();
        for(Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry : propsMap.entrySet()) {
            ImmutableBytesWritable key = entry.getKey();
            ImmutableBytesWritable globalValue = entry.getValue();
            Map<String, String> cfToPropertyValueMap = new HashMap<String, String>();
            Set<ImmutableBytesWritable> cfPropertyValueSet = new HashSet<ImmutableBytesWritable>();
            for(HColumnDescriptor columnDescriptor: columnDescriptors) {
                String columnFamilyName = Bytes.toString(columnDescriptor.getName());
                ImmutableBytesWritable value = columnDescriptor.getValues().get(key);
                // check if it is universal properties
                if (SYNCED_DATA_TABLE_AND_INDEX_COL_FAM_PROPERTIES.contains(Bytes.toString(key.get()))) {
                    definedProps.put(Bytes.toString(key.get()), Bytes.toString(value.get()));
                    break;
                }
                cfToPropertyValueMap.put(columnFamilyName, Bytes.toString(value.get()));
                cfPropertyValueSet.add(value);
            }
            if (cfPropertyValueSet.size() > 1) {
                for(Map.Entry<String, String> mapEntry: cfToPropertyValueMap.entrySet()) {
                    definedProps.put(String.format("%s.%s",  mapEntry.getKey(), Bytes.toString(key.get())), mapEntry.getValue());
                }
            } else {
                definedProps.put(Bytes.toString(key.get()), Bytes.toString(globalValue.get()));
            }
        }
    }

    private void setPTableProperties(PTable table) {
        Map <String, String> map = table.getPropertyValues();
        for(Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if(value != null) {
                definedProps.put(key, value);
            }
        }
    }

    private HTableDescriptor getTableDescriptor(ConnectionQueryServices cqsi, PTable table)
            throws SQLException, IOException {
        return cqsi.getAdmin().getTableDescriptor(
                TableName.valueOf(table.getPhysicalName().getString()));
    }

    private String convertPropertiesToString() {
        StringBuilder optionBuilder = new StringBuilder();
        for(Map.Entry<String, String> entry : definedProps.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String columnFamilyName = QueryConstants.DEFAULT_COLUMN_FAMILY;

            String[] colPropKey = key.split("\\.");
            if (colPropKey.length > 1) {
                columnFamilyName = colPropKey[0];
                key = colPropKey[1];
            }

            if(value!=null && defaultProps.get(key) != null && !value.equals(defaultProps.get(key))) {
                if (optionBuilder.length() != 0) {
                    optionBuilder.append(", ");
                }
                key = columnFamilyName.equals(QueryConstants.DEFAULT_COLUMN_FAMILY)?
                        key : String.format("\"%s\".%s", columnFamilyName, key);
                // properties value that corresponds to a number will not need single quotes around it
                // properties value that corresponds to a boolean value will not need single quotes around it
                if(!(StringUtils.isNumeric(value)) &&
                        !(value.equalsIgnoreCase(Boolean.TRUE.toString()) ||value.equalsIgnoreCase(Boolean.FALSE.toString()))) {
                    value= "'" + value + "'";
                }
                optionBuilder.append(key + "=" + value);
            }
        }
        return optionBuilder.toString();
    }

    private PTable getPTable(String pSchemaName, String pTableName) throws SQLException {
        String pTableFullName = SchemaUtil.getQualifiedTableName(pSchemaName, pTableName);
        return getPTable(pTableFullName);
    }

    private ConnectionQueryServices getCQSIObject() throws SQLException {
        try(Connection conn = getConnection()) {
            return conn.unwrap(PhoenixConnection.class).getQueryServices();
        }
    }

    public Connection getConnection() throws SQLException {
        if(tenantId!=null) {
            conf.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        return ConnectionUtil.getInputConnection(conf);
    }

    private String getColumnInfoStringForTable(PTable table) {
        StringBuilder colInfo = new StringBuilder();
        List<PColumn> columns = table.getBucketNum() == null ? table.getColumns() : table.getColumns().subList(1, table.getColumns().size());
        List<PColumn> pkColumns = table.getBucketNum() == null ? table.getPKColumns() : table.getColumns().subList(1, table.getPKColumns().size());

        return getColumnInfoString(table, colInfo, columns, pkColumns);
    }

    private String getColumnInfoString(PTable table, StringBuilder colInfo, List<PColumn> columns,
            List<PColumn> pkColumns) {
        ArrayList<String> colDefs = new ArrayList<>(columns.size());
        for (PColumn col : columns) {
            String def = extractColumn(col);
            if (pkColumns.size() == 1 && pkColumns.contains(col)) {
                def += " PRIMARY KEY" + extractPKColumnAttributes(col);
            }
            colDefs.add(def);
        }
        if (colDefs.size() > 0) {
            colInfo.append('(');
            colInfo.append(StringUtils.join(colDefs, ", "));
        }
        if (pkColumns.size() > 1) {
            // multi column primary key
            String
                    pkConstraint =
                    String.format(" CONSTRAINT %s PRIMARY KEY (%s)", table.getPKName().getString(),
                            extractPKConstraint(pkColumns));
            colInfo.append(pkConstraint);
        }
        if (colDefs.size() > 0) {
            colInfo.append(')');
        }
        return colInfo.toString();
    }

    private String getColumnInfoStringForView(PTable table, PTable baseTable) {
        StringBuilder colInfo = new StringBuilder();

        List<PColumn> columns = table.getColumns();
        List<PColumn> pkColumns = table.getPKColumns();

        Set<PColumn> columnSet = new HashSet<>(columns);
        Set<PColumn> pkSet = new HashSet<>(pkColumns);

        List<PColumn> baseColumns = baseTable.getColumns();
        List<PColumn> basePkColumns = baseTable.getPKColumns();

        Set<PColumn> baseColumnSet = new HashSet<>(baseColumns);
        Set<PColumn> basePkSet = new HashSet<>(basePkColumns);

        Set<PColumn> columnsSet = Sets.symmetricDifference(baseColumnSet, columnSet);
        Set<PColumn> pkColumnsSet = Sets.symmetricDifference(basePkSet, pkSet);

        columns = new ArrayList<>(columnsSet);
        pkColumns = new ArrayList<>(pkColumnsSet);


        return getColumnInfoString(table, colInfo, columns, pkColumns);
    }

    private String extractColumn(PColumn column) {
        String colName = column.getName().getString();
        if (column.getFamilyName() != null){
            String colFamilyName = column.getFamilyName().getString();
            // check if it is default column family name
            colName = colFamilyName.equals(QueryConstants.DEFAULT_COLUMN_FAMILY)? colName : String.format("\"%s\".\"%s\"", colFamilyName, colName);
        }
        boolean isArrayType = column.getDataType().isArrayType();
        String type = column.getDataType().getSqlTypeName();
        Integer maxLength = column.getMaxLength();
        Integer arrSize = column.getArraySize();
        Integer scale = column.getScale();
        StringBuilder buf = new StringBuilder(colName);
        buf.append(' ');

        if (isArrayType) {
            String arrayPrefix = type.split("\\s+")[0];
            buf.append(arrayPrefix);
            appendMaxLengthAndScale(buf, maxLength, scale);
            buf.append(' ');
            buf.append("ARRAY");
            if (arrSize != null) {
                buf.append('[');
                buf.append(arrSize);
                buf.append(']');
            }
        } else {
            buf.append(type);
            appendMaxLengthAndScale(buf, maxLength, scale);
        }

        if (!column.isNullable()) {
            buf.append(' ');
            buf.append("NOT NULL");
        }

        return buf.toString();
    }

    private void appendMaxLengthAndScale(StringBuilder buf, Integer maxLength, Integer scale){
        if (maxLength != null) {
            buf.append('(');
            buf.append(maxLength);
            if (scale != null) {
                buf.append(',');
                buf.append(scale); // has both max length and scale. For ex- decimal(10,2)
            }
            buf.append(')');
        }
    }

    private String extractPKColumnAttributes(PColumn column) {
        StringBuilder buf = new StringBuilder();

        if (column.getSortOrder() != SortOrder.getDefault()) {
            buf.append(' ');
            buf.append(column.getSortOrder().toString());
        }

        if (column.isRowTimestamp()) {
            buf.append(' ');
            buf.append("ROW_TIMESTAMP");
        }

        return buf.toString();
    }

    private String extractPKConstraint(List<PColumn> pkColumns) {
        ArrayList<String> colDefs = new ArrayList<>(pkColumns.size());
        for (PColumn pkCol : pkColumns) {
            colDefs.add(pkCol.getName().getString() + extractPKColumnAttributes(pkCol));
        }
        return StringUtils.join(colDefs, ", ");
    }

}
