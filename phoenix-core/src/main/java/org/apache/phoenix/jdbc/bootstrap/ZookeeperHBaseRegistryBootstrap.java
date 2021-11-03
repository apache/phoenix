package org.apache.phoenix.jdbc.bootstrap;

import com.google.common.base.Strings;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

import java.util.Map;

public class ZookeeperHBaseRegistryBootstrap extends HBaseRegistryBootstrap {

    public ZookeeperHBaseRegistryBootstrap(EmbeddedDriverContext edc) {
        super(edc);
    }

    @Override
    public String getStringForConnectionString() {
        return "";
    }

    @Override
    public HBaseRegistryBootstrap normalize() {
        return new ZookeeperHBaseRegistryBootstrap(this.getEmbeddedDriverContext());
    }

    @Override
    public HBaseRegistryBootstrapType getBootstrapType() {
        return HBaseRegistryBootstrapType.ZK;
    }

    @Override
    public Map<String, String> generateConnectionProps(EmbeddedDriverContext edc) {
        Map<String, String> connectionProps = Maps.newHashMapWithExpectedSize(3);

        if (!Strings.isNullOrEmpty(edc.getQuorum())) {
            connectionProps.put(QueryServices.ZOOKEEPER_QUORUM_ATTRIB, edc.getQuorum());
        }

        if (edc.getPort() != null) {
            connectionProps.put(QueryServices.ZOOKEEPER_PORT_ATTRIB, edc.getPort().toString());
        }

        if (!Strings.isNullOrEmpty(edc.getRootNode())) {
            connectionProps.put(QueryServices.ZOOKEEPER_ROOT_NODE_ATTRIB, edc.getRootNode());
        }

        return connectionProps;
    }
}
