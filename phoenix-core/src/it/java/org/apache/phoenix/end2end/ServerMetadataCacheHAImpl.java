package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.cache.ServerMetadataCacheImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link ServerMetadataCache} for HA tests.
 * Supports keeping for than one instance keyed on the config.
 */
public class ServerMetadataCacheHAImpl extends ServerMetadataCacheImpl {
    private static volatile Map<Configuration, ServerMetadataCacheHAImpl> INSTANCES = new HashMap<>();
    ServerMetadataCacheHAImpl(Configuration conf) {
        super(conf);
    }

    public static ServerMetadataCacheHAImpl getInstance(Configuration conf) {
        ServerMetadataCacheHAImpl result = INSTANCES.get(conf);
        if (result == null) {
            synchronized (ServerMetadataCacheHAImpl.class) {
                result = INSTANCES.get(conf);
                if (result == null) {
                    result = new ServerMetadataCacheHAImpl(conf);
                    INSTANCES.put(conf, result);
                }
            }
        }
        return result;
    }
}
