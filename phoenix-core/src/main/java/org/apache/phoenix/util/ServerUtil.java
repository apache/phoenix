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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.InterRegionServerIndexRpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.HashJoinCacheNotFoundException;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerUtil {
    private static final int COPROCESSOR_SCAN_WORKS = VersionUtil.encodeVersion("0.98.6");
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerUtil.class);
    
    private static final String FORMAT = "ERROR %d (%s): %s";
    private static final Pattern PATTERN = Pattern.compile("ERROR (\\d+) \\((\\w+)\\): (.*)");
    private static final Pattern HASH_JOIN_EXCEPTION_PATTERN = Pattern.compile("joinId: (-?\\d+)");
    private static final Pattern PATTERN_FOR_TS = Pattern.compile(",serverTimestamp=(\\d+),");
    private static final String FORMAT_FOR_TIMESTAMP = ",serverTimestamp=%d,";
    private static final Map<Class<? extends Exception>, SQLExceptionCode> errorcodeMap
        = new HashMap<Class<? extends Exception>, SQLExceptionCode>();
    static {
        // Map a normal exception into a corresponding SQLException.
        errorcodeMap.put(ArithmeticException.class, SQLExceptionCode.SERVER_ARITHMETIC_ERROR);
    }

    public static void throwIOException(String msg, Throwable t) throws IOException {
        throw createIOException(msg, t);
    }

    public static IOException createIOException(String msg, Throwable t) {
        // First unwrap SQLExceptions if it's root cause is an IOException.
        if (t instanceof SQLException) {
            Throwable cause = t.getCause();
            if (cause instanceof IOException) {
                t = cause;
            }
        }
        // Throw immediately if DoNotRetryIOException
        if (t instanceof DoNotRetryIOException) {
            return (DoNotRetryIOException) t;
        } else if (t instanceof IOException) {
            // If the IOException does not wrap any exception, then bubble it up.
            Throwable cause = t.getCause();
            if (cause instanceof RetriesExhaustedWithDetailsException)
            	return new DoNotRetryIOException(t.getMessage(), cause);
            else if (cause == null || cause instanceof IOException) {
                return (IOException) t;
            }
            // Else assume it's been wrapped, so throw as DoNotRetryIOException to prevent client hanging while retrying
            return new DoNotRetryIOException(t.getMessage(), cause);
        } else if (t instanceof SQLException) {
            // If it's already an SQLException, construct an error message so we can parse and reconstruct on the client side.
            return new DoNotRetryIOException(constructSQLErrorMessage((SQLException) t, msg), t);
        } else {
            // Not a DoNotRetryIOException, IOException or SQLException. Map the exception type to a general SQLException 
            // and construct the error message so it can be reconstruct on the client side.
            //
            // If no mapping exists, rethrow it as a generic exception.
            SQLExceptionCode code = errorcodeMap.get(t.getClass());
            if (code == null) {
                return new DoNotRetryIOException(msg + ": " + t.getMessage(), t);
            } else {
                return new DoNotRetryIOException(constructSQLErrorMessage(code, t, msg), t);
            }
        }
    }

    private static String constructSQLErrorMessage(SQLExceptionCode code, Throwable e, String message) {
        return constructSQLErrorMessage(code.getErrorCode(), code.getSQLState(), code.getMessage() + " " + e.getMessage() + " " + message);
    }

    private static String constructSQLErrorMessage(SQLException e, String message) {
        return constructSQLErrorMessage(e.getErrorCode(), e.getSQLState(), e.getMessage() + " " + message);
    }

    private static String constructSQLErrorMessage(int errorCode, String SQLState, String message) {
        return String.format(FORMAT, errorCode, SQLState, message);
    }

    public static SQLException parseServerException(Throwable t) {
        SQLException e = parseServerExceptionOrNull(t);
        if (e != null) {
            return e;
        }
        return new PhoenixIOException(t);
    }

    /**
     * Return the first SQLException in the exception chain, otherwise parse it.
     * When we're receiving an exception locally, there's no need to string parse,
     * as the SQLException will already be part of the chain.
     * @param t
     * @return the SQLException, or null if none found
     */
    public static SQLException parseLocalOrRemoteServerException(Throwable t) {
        while (t.getCause() != null) {
            if (t instanceof NotServingRegionException) {
                return parseRemoteException(new StaleRegionBoundaryCacheException());
            } else if (t instanceof SQLException) {
                return (SQLException) t;
            }
            t = t.getCause();
        }
        return parseRemoteException(t);
    }
    
    public static SQLException parseServerExceptionOrNull(Throwable t) {
        while (t.getCause() != null) {
            if (t instanceof NotServingRegionException) {
                return parseRemoteException(new StaleRegionBoundaryCacheException());
            }
            t = t.getCause();
        }
        return parseRemoteException(t);
    }

    private static SQLException parseRemoteException(Throwable t) {
        
        String message = t.getLocalizedMessage();
        if (message != null) {
            // If the message matches the standard pattern, recover the SQLException and throw it.
            Matcher matcher = PATTERN.matcher(t.getLocalizedMessage());
            if (matcher.find()) {
                int statusCode = Integer.parseInt(matcher.group(1));
                SQLExceptionCode code = SQLExceptionCode.fromErrorCode(statusCode);
                if(code.equals(SQLExceptionCode.HASH_JOIN_CACHE_NOT_FOUND)){
                    Matcher m = HASH_JOIN_EXCEPTION_PATTERN.matcher(t.getLocalizedMessage());
                    if (m.find()) { return new HashJoinCacheNotFoundException(Long.parseLong(m.group(1))); }
                }
                return new SQLExceptionInfo.Builder(code).setMessage(matcher.group()).setRootCause(t).build().buildException();
            }
        }
        return null;
    }

    private static boolean coprocessorScanWorks(RegionCoprocessorEnvironment env) {
        return (VersionUtil.encodeVersion(env.getHBaseVersion()) >= COPROCESSOR_SCAN_WORKS);
    }
    
    /*
     * This code works around HBASE-11837 which causes HTableInterfaces retrieved from
     * RegionCoprocessorEnvironment to not read local data.
     */
    private static Table getTableFromSingletonPool(RegionCoprocessorEnvironment env, TableName tableName) throws IOException {
        // It's ok to not ever do a pool.close() as we're storing a single
        // table only. The HTablePool holds no other resources that this table
        // which will be closed itself when it's no longer needed.
        Connection conn = ConnectionFactory.getConnection(ConnectionType.DEFAULT_SERVER_CONNECTION, env);
        try {
            return conn.getTable(tableName);
        } catch (RuntimeException t) {
            // handle cases that an IOE is wrapped inside a RuntimeException like HTableInterface#createHTableInterface
            if(t.getCause() instanceof IOException) {
                throw (IOException)t.getCause();
            } else {
                throw t;
            }
        }
    }
    
    public static Table getHTableForCoprocessorScan (RegionCoprocessorEnvironment env,
                                                               Table writerTable) throws IOException {
        if (coprocessorScanWorks(env)) {
            return writerTable;
        }
        return getTableFromSingletonPool(env, writerTable.getName());
    }
    
    public static Table getHTableForCoprocessorScan (RegionCoprocessorEnvironment env, TableName tableName) throws IOException {
        if (coprocessorScanWorks(env)) {
            return env.getConnection().getTable(tableName);
        }
        return getTableFromSingletonPool(env, tableName);
    }
    
    public static long parseServerTimestamp(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return parseTimestampFromRemoteException(t);
    }

    public static long parseTimestampFromRemoteException(Throwable t) {
        String message = t.getLocalizedMessage();
        if (message != null) {
            // If the message matches the standard pattern, recover the SQLException and throw it.
            Matcher matcher = PATTERN_FOR_TS.matcher(t.getLocalizedMessage());
            if (matcher.find()) {
                String tsString = matcher.group(1);
                if (tsString != null) {
                    return Long.parseLong(tsString);
                }
            }
        }
        return HConstants.LATEST_TIMESTAMP;
    }

    public static DoNotRetryIOException wrapInDoNotRetryIOException(String msg, Throwable t, long timestamp) {
        if (msg == null) {
            msg = "";
        }
        if (t instanceof SQLException) {
            msg = t.getMessage() + " " + msg;
        }
        msg += String.format(FORMAT_FOR_TIMESTAMP, timestamp);
        return new DoNotRetryIOException(msg, t);
    }
    
    public static boolean readyToCommit(int rowCount, long mutationSize, int maxBatchSize, long maxBatchSizeBytes) {
        return maxBatchSize > 0 && rowCount >= maxBatchSize
                || (maxBatchSizeBytes > 0 && mutationSize >= maxBatchSizeBytes);
    }
    
    public static boolean isKeyInRegion(byte[] key, Region region) {
        byte[] startKey = region.getRegionInfo().getStartKey();
        byte[] endKey = region.getRegionInfo().getEndKey();
        return (Bytes.compareTo(startKey, key) <= 0
                && (Bytes.compareTo(HConstants.LAST_ROW, endKey) == 0 || Bytes.compareTo(key,
                    endKey) < 0));
    }

    public static RowLock acquireLock(Region region, byte[] key, List<RowLock> locks)
            throws IOException {
        RowLock rowLock = region.getRowLock(key, false);
        if (rowLock == null) {
            throw new IOException("Failed to acquire lock on " + Bytes.toStringBinary(key));
        }
        if (locks != null) {
            locks.add(rowLock);
        }
        return rowLock;
    }

    public static void releaseRowLocks(List<RowLock> rowLocks) {
        if (rowLocks != null) {
            for (RowLock rowLock : rowLocks) {
                rowLock.release();
            }
            rowLocks.clear();
        }
    }

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

    public static <T> Throwable getExceptionFromFailedFuture(Future<T> f) {
        Throwable t = null;
        try {
            f.get();
        } catch (Exception e) {
            t = e;
        }
        return t;
    }
}
