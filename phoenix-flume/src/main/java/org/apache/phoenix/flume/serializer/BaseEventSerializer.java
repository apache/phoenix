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
package org.apache.phoenix.flume.serializer;

import static org.apache.phoenix.flume.FlumeConstants.CONFIG_COLUMN_NAMES;
import static org.apache.phoenix.flume.FlumeConstants.CONFIG_HEADER_NAMES;
import static org.apache.phoenix.flume.FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR;
import static org.apache.phoenix.flume.FlumeConstants.DEFAULT_COLUMNS_DELIMITER;
import static org.apache.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.flume.DefaultKeyGenerator;
import org.apache.phoenix.flume.FlumeConstants;
import org.apache.phoenix.flume.KeyGenerator;
import org.apache.phoenix.flume.SchemaHandler;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public abstract class BaseEventSerializer implements EventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(BaseEventSerializer.class);
    
    protected Connection connection;
    protected String fullTableName;
    protected ColumnInfo[] columnMetadata;
    protected boolean autoGenerateKey = false;
    protected KeyGenerator  keyGenerator;
    protected List<String>  colNames = Lists.newArrayListWithExpectedSize(10);
    protected List<String>  headers  = Lists.newArrayListWithExpectedSize(5);
    protected String upsertStatement;
    private   String jdbcUrl;
    private   Integer batchSize;
    private   String  createTableDdl;


    
    
    
    @Override
    public void configure(Context context) {
        
        this.createTableDdl = context.getString(FlumeConstants.CONFIG_TABLE_DDL);
        this.fullTableName = context.getString(FlumeConstants.CONFIG_TABLE);
        final String zookeeperQuorum = context.getString(FlumeConstants.CONFIG_ZK_QUORUM);
        final String ipJdbcURL = context.getString(FlumeConstants.CONFIG_JDBC_URL);
        this.batchSize = context.getInteger(FlumeConstants.CONFIG_BATCHSIZE, FlumeConstants.DEFAULT_BATCH_SIZE);
        final String columnNames = context.getString(CONFIG_COLUMN_NAMES);
        final String headersStr = context.getString(CONFIG_HEADER_NAMES);
        final String keyGeneratorType = context.getString(CONFIG_ROWKEY_TYPE_GENERATOR);
       
        Preconditions.checkNotNull(this.fullTableName,"Table name cannot be empty, please specify in the configuration file");
        if(!Strings.isNullOrEmpty(zookeeperQuorum)) {
            this.jdbcUrl = QueryUtil.getUrl(zookeeperQuorum);
        }
        if(!Strings.isNullOrEmpty(ipJdbcURL)) {
            this.jdbcUrl = ipJdbcURL;
        }
        Preconditions.checkNotNull(this.jdbcUrl,"Please specify either the zookeeper quorum or the jdbc url in the configuration file");
        Preconditions.checkNotNull(columnNames,"Column names cannot be empty, please specify in configuration file");
        for(String s : Splitter.on(DEFAULT_COLUMNS_DELIMITER).split(columnNames)) {
           colNames.add(s);
        }
        
         if(!Strings.isNullOrEmpty(headersStr)) {
            for(String s : Splitter.on(DEFAULT_COLUMNS_DELIMITER).split(headersStr)) {
                headers.add(s);
             }
        }
      
        if(!Strings.isNullOrEmpty(keyGeneratorType)) {
            try {
                keyGenerator =  DefaultKeyGenerator.valueOf(keyGeneratorType.toUpperCase());
                this.autoGenerateKey = true;
            } catch(IllegalArgumentException iae) {
                logger.error("An invalid key generator {} was specified in configuration file. Specify one of {}",keyGeneratorType,DefaultKeyGenerator.values());
                Throwables.propagate(iae);
            } 
        }
        
        logger.debug(" the jdbcUrl configured is {}",jdbcUrl);
        logger.debug(" columns configured are {}",colNames.toString());
        logger.debug(" headers configured are {}",headersStr);
        logger.debug(" the keyGenerator configured is {} ",keyGeneratorType);

        doConfigure(context);
        
    }
    
    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP
        
    }
    
    
    @Override
    public void initialize() throws SQLException {
        final Properties props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, String.valueOf(this.batchSize)); 
        ResultSet rs = null;
        try {
            this.connection = DriverManager.getConnection(this.jdbcUrl, props);
            this.connection.setAutoCommit(false);
            if(this.createTableDdl != null) {
                 SchemaHandler.createTable(connection,createTableDdl);
            }
      
            
            final Map<String,Integer> qualifiedColumnMap = Maps.newLinkedHashMap();
            final Map<String,Integer> unqualifiedColumnMap = Maps.newLinkedHashMap();
            final String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            final String tableName  = SchemaUtil.getTableNameFromFullName(fullTableName);
            
            String rowkey = null;
            String  cq = null;
            String  cf = null;
            Integer dt = null;
            rs = connection.getMetaData().getColumns("", StringUtil.escapeLike(SchemaUtil.normalizeIdentifier(schemaName)), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier(tableName)), null);
            while (rs.next()) {
                cf = rs.getString(QueryUtil.COLUMN_FAMILY_POSITION);
                cq = rs.getString(QueryUtil.COLUMN_NAME_POSITION);
                dt = rs.getInt(QueryUtil.DATA_TYPE_POSITION);
                if(Strings.isNullOrEmpty(cf)) {
                    rowkey = cq; // this is required only when row key is auto generated
                } else {
                    qualifiedColumnMap.put(SchemaUtil.getColumnDisplayName(cf, cq), dt);
                }
                unqualifiedColumnMap.put(SchemaUtil.getColumnDisplayName(null, cq), dt);
             }
            
            //can happen when table not found in Hbase.
            if(unqualifiedColumnMap.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TABLE_UNDEFINED)
                        .setTableName(tableName).build().buildException();
            }
       
            int colSize = colNames.size();
            int headersSize = headers.size();
            int totalSize = colSize + headersSize + ( autoGenerateKey ? 1 : 0);
            columnMetadata = new ColumnInfo[totalSize] ;
           
            int position = 0;
            position = this.addToColumnMetadataInfo(colNames, qualifiedColumnMap, unqualifiedColumnMap, position);
            position = this.addToColumnMetadataInfo(headers,  qualifiedColumnMap, unqualifiedColumnMap, position);
           
            if(autoGenerateKey) {
                Integer sqlType = unqualifiedColumnMap.get(rowkey);
                if (sqlType == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                         .setColumnName(rowkey).setTableName(fullTableName).build().buildException();
                }
                columnMetadata[position] = new ColumnInfo(rowkey, sqlType);
                position++;
            }
            
            this.upsertStatement = QueryUtil.constructUpsertStatement(fullTableName, Arrays.asList(columnMetadata));
            logger.info(" the upsert statement is {} " ,this.upsertStatement);
            
        }  catch (SQLException e) {
            logger.error("error {} occurred during initializing connection ",e.getMessage());
            throw e;
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        doInitialize();
    }
    
    private int addToColumnMetadataInfo(final List<String> columns , final Map<String,Integer> qualifiedColumnsInfoMap, Map<String, Integer> unqualifiedColumnsInfoMap, int position) throws SQLException {
        Preconditions.checkNotNull(columns);
        Preconditions.checkNotNull(qualifiedColumnsInfoMap);
        Preconditions.checkNotNull(unqualifiedColumnsInfoMap);
       for (int i = 0 ; i < columns.size() ; i++) {
            String columnName = SchemaUtil.normalizeIdentifier(columns.get(i).trim());
            Integer sqlType = unqualifiedColumnsInfoMap.get(columnName);
            if (sqlType == null) {
                sqlType = qualifiedColumnsInfoMap.get(columnName);
                if (sqlType == null) {
                   throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                        .setColumnName(columnName).setTableName(this.fullTableName).build().buildException();
                }
            }
            columnMetadata[position] = new ColumnInfo(columnName, sqlType);
            position++;
       }
       return position;
    }
    
    public abstract void doConfigure(Context context);
    
    public abstract void doInitialize() throws SQLException;
    
    
    @Override
    public void close() {
        if(connection != null) {
            try {
               connection.close();
         } catch (SQLException e) {
            logger.error(" Error while closing connection {} ");
         }
       }
    }
}
