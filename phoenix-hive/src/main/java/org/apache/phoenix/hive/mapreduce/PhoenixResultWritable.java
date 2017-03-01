/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.mapreduce;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.hive.PhoenixRowKey;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.util.ColumnMappingUtils;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.util.ColumnInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Serialized class for SerDe
 *
 */
public class PhoenixResultWritable implements Writable, DBWritable, Configurable {

    private static final Log LOG = LogFactory.getLog(PhoenixResultWritable.class);

    private List<ColumnInfo> columnMetadataList;
    private List<Object> valueList;    // for output
    private Map<String, Object> rowMap = Maps.newHashMap();  // for input
    private Map<String, String> columnMap;

    private int columnCount = -1;

    private Configuration config;
    private boolean isTransactional;
    private Map<String, Object> rowKeyMap = Maps.newLinkedHashMap();
    private List<String> primaryKeyColumnList;

    public PhoenixResultWritable() {
    }

    public PhoenixResultWritable(Configuration config) throws IOException {
        setConf(config);
    }

    public PhoenixResultWritable(Configuration config, List<ColumnInfo> columnMetadataList)
            throws IOException {
        this(config);
        this.columnMetadataList = columnMetadataList;
        valueList = Lists.newArrayListWithExpectedSize(columnMetadataList.size());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    // for write
    public void clear() {
        valueList.clear();
    }

    // for write
    public void add(Object value) {
        valueList.add(value);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        ColumnInfo columnInfo = null;
        Object value = null;

        try {
            for (int i = 0, limit = columnMetadataList.size(); i < limit; i++) {
                columnInfo = columnMetadataList.get(i);

                if (valueList.size() > i) {
                    value = valueList.get(i);
                } else {
                    value = null;
                }

                if (value == null) {
                    statement.setNull(i + 1, columnInfo.getSqlType());
                } else {
                    statement.setObject(i + 1, value, columnInfo.getSqlType());
                }
            }
        } catch (SQLException | RuntimeException e) {
            LOG.error("[column-info, value] : " + columnInfo + ", " + value);
            throw e;
        }
    }

    public void delete(PreparedStatement statement) throws SQLException {
        ColumnInfo columnInfo = null;
        Object value = null;

        try {
            for (int i = 0, limit = primaryKeyColumnList.size(); i < limit; i++) {
                columnInfo = columnMetadataList.get(i);

                if (valueList.size() > i) {
                    value = valueList.get(i);
                } else {
                    value = null;
                }

                if (value == null) {
                    statement.setNull(i + 1, columnInfo.getSqlType());
                } else {
                    statement.setObject(i + 1, value, columnInfo.getSqlType());
                }
            }
        } catch (SQLException | RuntimeException e) {
            LOG.error("[column-info, value] : " + columnInfo + ", " + value);
            throw e;
        }
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        if (columnCount == -1) {
            this.columnCount = rsmd.getColumnCount();
        }
        rowMap.clear();

        for (int i = 0; i < columnCount; i++) {
            Object value = resultSet.getObject(i + 1);
            String columnName = rsmd.getColumnName(i + 1);
            String mapName = columnMap.get(columnName);
            if(mapName != null) {
                columnName = mapName;
            }
            rowMap.put(columnName, value);
        }

        // Adding row__id column.
        if (isTransactional) {
            rowKeyMap.clear();

            for (String pkColumn : primaryKeyColumnList) {
                rowKeyMap.put(pkColumn, rowMap.get(pkColumn));
            }
        }
    }

    public void readPrimaryKey(PhoenixRowKey rowKey) {
        rowKey.setRowKeyMap(rowKeyMap);
    }

    public List<ColumnInfo> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void setColumnMetadataList(List<ColumnInfo> columnMetadataList) {
        this.columnMetadataList = columnMetadataList;
    }

    public Map<String, Object> getResultMap() {
        return rowMap;
    }

    public List<Object> getValueList() {
        return valueList;
    }

    @Override
    public void setConf(Configuration conf) {
        config = conf;
        this.columnMap = ColumnMappingUtils.getReverseColumnMapping(config.get(PhoenixStorageHandlerConstants.PHOENIX_COLUMN_MAPPING,""));

        isTransactional = PhoenixStorageHandlerUtil.isTransactionalTable(config);

        if (isTransactional) {
            primaryKeyColumnList = PhoenixUtil.getPrimaryKeyColumnList(config, config.get
                    (PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME));
        }
    }

    @Override
    public Configuration getConf() {
        return config;
    }
}
