package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.phoenix.cache.ServerMetadataCacheImpl;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of {@link ServerMetadataCache} for Integration Tests.
 * Supports keeping more than one instance keyed on the regionserver ServerName.
 */
public class ServerMetadataCacheTestImpl extends ServerMetadataCacheImpl {
    private static volatile Map<ServerName, ServerMetadataCacheTestImpl> INSTANCES = new HashMap<>();
    private Connection connectionForTesting;

    ServerMetadataCacheTestImpl(Configuration conf) {
        super(conf);
    }

    public static ServerMetadataCacheTestImpl getInstance(Configuration conf, ServerName serverName) {
        ServerMetadataCacheTestImpl result = INSTANCES.get(serverName);
        if (result == null) {
            synchronized (ServerMetadataCacheTestImpl.class) {
                result = INSTANCES.get(serverName);
                if (result == null) {
                    result = new ServerMetadataCacheTestImpl(conf);
                    INSTANCES.put(serverName, result);
                }
            }
        }
        return result;
    }

    public static void setInstance(ServerName serverName, ServerMetadataCacheTestImpl cache) {
        INSTANCES.put(serverName, cache);
    }

    public Long getLastDDLTimestampForTableFromCacheOnly(byte[] tenantID, byte[] schemaName,
                                                         byte[] tableName) {
        byte[] tableKey = SchemaUtil.getTableKey(tenantID, schemaName, tableName);
        ImmutableBytesPtr tableKeyPtr = new ImmutableBytesPtr(tableKey);
        return lastDDLTimestampMap.getIfPresent(tableKeyPtr);
    }

    public void setConnectionForTesting(Connection connection) {
        this.connectionForTesting = connection;
    }

    public static void resetCache() {
        INSTANCES.clear();
    }

    @Override
    protected Connection getConnection(Properties properties) throws SQLException {
        return connectionForTesting != null ? connectionForTesting
                : super.getConnection(properties);
    }
}
