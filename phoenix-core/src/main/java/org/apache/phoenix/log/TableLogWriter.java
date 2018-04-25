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
package org.apache.phoenix.log;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ImmutableMap;

/**
 * Writes RingBuffer log event into table 
 * 
 */
public class TableLogWriter implements LogWriter {
    private static final Log LOG = LogFactory.getLog(LogWriter.class);
    private Connection connection;
    private boolean isClosed;
    private Table table;
    private Configuration config;

    public TableLogWriter(Configuration configuration) {
        this.config = configuration;
        try {
            this.connection = ConnectionFactory.createConnection(configuration);
            table = this.connection.getTable(SchemaUtil.getPhysicalTableName(
                    SchemaUtil.getTableNameAsBytes(SYSTEM_CATALOG_SCHEMA, SYSTEM_LOG_TABLE), config));
        } catch (Exception e) {
            LOG.warn("Unable to initiate LogWriter for writing query logs to table");
        }
    }

    @Override
    public void write(RingBufferEvent event) throws SQLException, IOException {
        if(isClosed()){
            LOG.warn("Unable to commit query log as Log committer is already closed");
            return;
        }
        if (table == null || connection == null) {
            LOG.warn("Unable to commit query log as connection was not initiated ");
            return;
        }
        ImmutableMap<QueryLogInfo, Object> queryInfo=event.getQueryInfo();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        Put put =new Put(Bytes.toBytes(event.getQueryId()));
        for (Entry<QueryLogInfo, Object> entry : queryInfo.entrySet()) {
            if (entry.getKey().logLevel.ordinal() <= event.getConnectionLogLevel().ordinal()) {
                LiteralExpression expression = LiteralExpression.newConstant(entry.getValue(), entry.getKey().dataType,
                        Determinism.ALWAYS);
                expression.evaluate(null, ptr);
                put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, Bytes.toBytes(entry.getKey().columnName),
                        ByteUtil.copyKeyBytesIfNecessary(ptr));
            }
        }
        
        if (QueryLogInfo.QUERY_STATUS_I.logLevel.ordinal() <= event.getConnectionLogLevel().ordinal()
                && (event.getLogState() == QueryLogState.COMPLETED || event.getLogState() == QueryLogState.FAILED)) {
            LiteralExpression expression = LiteralExpression.newConstant(event.getLogState().toString(),
                    QueryLogInfo.QUERY_STATUS_I.dataType, Determinism.ALWAYS);
            expression.evaluate(null, ptr);
            put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                    Bytes.toBytes(QueryLogInfo.QUERY_STATUS_I.columnName), ByteUtil.copyKeyBytesIfNecessary(ptr));
        }
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        table.put(put);
        
    }
    
    @Override
    public void close() throws IOException {
        if(isClosed()){
            return;
        }
        isClosed=true;
        try {
            if (table != null) {
                table.close();
            }
            if (connection != null && !connection.isClosed()) {
                //It should internally close all the statements
                connection.close();
            }
        } catch (IOException e) {
            // TODO Ignore?
        }
    }
    
    public boolean isClosed(){
        return isClosed;
    }

}
