package org.apache.phoenix.jdbc.bootstrapz;

public class HBaseRegistryBootstrapFactory {

    public static HBaseRegistryBootstrap resolve(HBaseRegistryBootstrapType theType) {
        if (theType == HBaseRegistryBootstrapType.HRPC) {
            return new HRpcHBaseRegistryBootstrap();
        }

        return new ZookeeperHBaseRegistryBootstrap();
    }
}
