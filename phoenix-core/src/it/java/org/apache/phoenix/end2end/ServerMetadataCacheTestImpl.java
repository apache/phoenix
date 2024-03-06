package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.phoenix.cache.ServerMetadataCacheImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link ServerMetadataCache} for Integration Tests.
 * Supports keeping more than one instance keyed on the regionserver ServerName.
 */
public class ServerMetadataCacheTestImpl extends ServerMetadataCacheImpl {
    private static volatile Map<ServerName, ServerMetadataCacheTestImpl> INSTANCES = new HashMap<>();
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
}
