package org.apache.phoenix.jdbc.bootstrap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MasterRegistry;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HRpcHBaseRegistryBootstrap extends HBaseRegistryBootstrap {

    public HRpcHBaseRegistryBootstrap(EmbeddedDriverContext edc) {
        super(edc);
    }

    @Override
    public HBaseRegistryBootstrap normalize() {
        return new HRpcHBaseRegistryBootstrap(this.getEmbeddedDriverContext());
    }

    @Override
    public HBaseRegistryBootstrapType getBootstrapType() {
        return HBaseRegistryBootstrapType.HRPC;
    }

    @Override
    public Map<String, String> generateConnectionProps(EmbeddedDriverContext edc) {
        Map<String, String> connectionProps = Maps.newHashMapWithExpectedSize(3);

        if (edc.getQuorum() != null) {
            final String[] masters = edc.getQuorum().split(",");

            String masterPort;

            if (edc.getPort() != null) {
                masterPort = edc.getPort().toString();
            } else {
                masterPort = HBaseFactoryProvider.getConfigurationFactory()
                        .getConfiguration().get(HConstants.MASTER_PORT);
            }

            final List<String> masterList = new ArrayList<>();
            for (final String m : masters) {
                masterList.add(m + ":" + masterPort);
            }

            connectionProps.put(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, MasterRegistry.class.getName());
            connectionProps.put(HConstants.MASTER_ADDRS_KEY, String.join(",", masterList));
        }

        return connectionProps;
    }
}
