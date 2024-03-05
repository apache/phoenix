package org.apache.phoenix.end2end;

import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;

public class PhoenixRegionServerEndpointHAImpl extends PhoenixRegionServerEndpoint {
    @Override
    public ServerMetadataCache getServerMetadataCache() {
        return ServerMetadataCacheHAImpl.getInstance(conf);
    }
}
