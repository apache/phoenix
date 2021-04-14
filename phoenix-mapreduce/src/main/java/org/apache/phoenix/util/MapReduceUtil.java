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

import static org.apache.phoenix.hbase.index.write.IndexWriterUtils.DEFAULT_INDEX_WRITER_RPC_PAUSE;
import static org.apache.phoenix.hbase.index.write.IndexWriterUtils.DEFAULT_INDEX_WRITER_RPC_RETRIES_NUMBER;
import static org.apache.phoenix.hbase.index.write.IndexWriterUtils.INDEX_WRITER_RPC_PAUSE;
import static org.apache.phoenix.hbase.index.write.IndexWriterUtils.INDEX_WRITER_RPC_RETRIES_NUMBER;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.InterRegionServerIndexRpcControllerFactory;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceUtil extends ClientUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceUtil.class);
    
    public static enum ConnectionType {
        COMPACTION_CONNECTION,
        INDEX_WRITER_CONNECTION,
        INDEX_WRITER_CONNECTION_WITH_CUSTOM_THREADS,
        INDEX_WRITER_CONNECTION_WITH_CUSTOM_THREADS_NO_RETRIES,
        DEFAULT_SERVER_CONNECTION;
    }

    public static class ConnectionFactory {
        
        private static Map<ConnectionType, Connection> connections =
                new ConcurrentHashMap<ConnectionType, Connection>();

        public static Connection getConnection(final ConnectionType connectionType, final RegionCoprocessorEnvironment env) {
            return connections.computeIfAbsent(connectionType, new Function<ConnectionType, Connection>() {
                @Override
                    public Connection apply(ConnectionType t) {
                    try {
                        return env.createConnection(getTypeSpecificConfiguration(connectionType, env.getConfiguration()));
                    } catch (IOException e) {
                       throw new RuntimeException(e);
                    }
                }
            });
        }

        public static Configuration getTypeSpecificConfiguration(ConnectionType connectionType, Configuration conf) {
            switch (connectionType) {
            case COMPACTION_CONNECTION:
                return getCompactionConfig(conf);
            case DEFAULT_SERVER_CONNECTION:
                return conf;
            case INDEX_WRITER_CONNECTION:
                return getIndexWriterConnection(conf);
            case INDEX_WRITER_CONNECTION_WITH_CUSTOM_THREADS:
                return getIndexWriterConfigurationWithCustomThreads(conf);
            case INDEX_WRITER_CONNECTION_WITH_CUSTOM_THREADS_NO_RETRIES:
                return getNoRetriesIndexWriterConfigurationWithCustomThreads(conf);
            default:
                return conf;
            }
         }
        
        public static void shutdown() {
            synchronized (ConnectionFactory.class) {
                for (Connection connection : connections.values()) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        LOGGER.warn("Unable to close coprocessor connection", e);
                    }
                }
                connections.clear();
            }
        }

        public static int getConnectionsCount() {
            return connections.size();
        }

     }

    public static Configuration getCompactionConfig(Configuration conf) {
        Configuration compactionConfig = PropertiesUtil.cloneConfig(conf);
        // lower the number of rpc retries, so we don't hang the compaction
        compactionConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
            conf.getInt(QueryServices.METADATA_WRITE_RETRIES_NUMBER,
                QueryServicesOptions.DEFAULT_METADATA_WRITE_RETRIES_NUMBER));
        compactionConfig.setInt(HConstants.HBASE_CLIENT_PAUSE,
            conf.getInt(QueryServices.METADATA_WRITE_RETRY_PAUSE,
                QueryServicesOptions.DEFAULT_METADATA_WRITE_RETRY_PAUSE));
        return compactionConfig;
    }

    public static Configuration getIndexWriterConnection(Configuration conf) {
        Configuration clonedConfig = PropertiesUtil.cloneConfig(conf);
        /*
         * Set the rpc controller factory so that the HTables used by IndexWriter would
         * set the correct priorities on the remote RPC calls.
         */
        clonedConfig.setClass(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
                InterRegionServerIndexRpcControllerFactory.class, RpcControllerFactory.class);
        // lower the number of rpc retries.  We inherit config from HConnectionManager#setServerSideHConnectionRetries,
        // which by default uses a multiplier of 10.  That is too many retries for our synchronous index writes
        clonedConfig.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
            conf.getInt(INDEX_WRITER_RPC_RETRIES_NUMBER,
                DEFAULT_INDEX_WRITER_RPC_RETRIES_NUMBER));
        clonedConfig.setInt(HConstants.HBASE_CLIENT_PAUSE, conf
            .getInt(INDEX_WRITER_RPC_PAUSE, DEFAULT_INDEX_WRITER_RPC_PAUSE));
        return clonedConfig;
    }

    public static Configuration getIndexWriterConfigurationWithCustomThreads(Configuration conf) {
        Configuration clonedConfig = getIndexWriterConnection(conf);
        setHTableThreads(clonedConfig);
        return clonedConfig;
    }

    private static void setHTableThreads(Configuration conf) {
        // set the number of threads allowed per table.
        int htableThreads =
                conf.getInt(IndexWriterUtils.INDEX_WRITER_PER_TABLE_THREADS_CONF_KEY,
                    IndexWriterUtils.DEFAULT_NUM_PER_TABLE_THREADS);
        IndexManagementUtil.setIfNotSet(conf, IndexWriterUtils.HTABLE_THREAD_KEY, htableThreads);
    }
    
    public static Configuration getNoRetriesIndexWriterConfigurationWithCustomThreads(Configuration conf) {
        Configuration clonedConf = getIndexWriterConfigurationWithCustomThreads(conf);
        // note in HBase 2+, numTries = numRetries + 1
        // in prior versions, numTries = numRetries
        clonedConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
        return clonedConf;

    }
}
