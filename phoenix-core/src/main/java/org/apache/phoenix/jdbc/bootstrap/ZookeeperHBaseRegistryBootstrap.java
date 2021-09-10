package org.apache.phoenix.jdbc.bootstrap;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import java.util.Map;

public class ZookeeperHBaseRegistryBootstrap extends HBaseRegistryBootstrap {
    public ZookeeperHBaseRegistryBootstrap() {
        super(HBaseRegistryBootstrapType.ZK);
    }

    @Override
    public Map<String, String> generateConnectionProps(String quorum, Integer port, String rootNode) {
        Map<String, String> connectionProps = Maps.newHashMapWithExpectedSize(3);

        if (quorum != null) {
            connectionProps.put(QueryServices.ZOOKEEPER_QUORUM_ATTRIB, quorum);
        }

        if (port != null) {
            connectionProps.put(QueryServices.ZOOKEEPER_PORT_ATTRIB, port.toString());
        }

        if (rootNode != null) {
            connectionProps.put(QueryServices.ZOOKEEPER_ROOT_NODE_ATTRIB, rootNode);
        }

        return connectionProps;
    }
}
