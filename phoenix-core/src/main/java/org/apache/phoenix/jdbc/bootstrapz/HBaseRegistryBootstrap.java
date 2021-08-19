package org.apache.phoenix.jdbc.bootstrapz;

import java.util.Map;

public abstract class HBaseRegistryBootstrap {
    private final HBaseRegistryBootstrapType bootstrapType;

    public HBaseRegistryBootstrap(HBaseRegistryBootstrapType bootstrapType) {
        this.bootstrapType = bootstrapType;
    }

    public abstract Map<String, String> generateConnectionProps(String quorum, Integer port, String rootNode);



    @Override
    public int hashCode() {
        return bootstrapType.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HBaseRegistryBootstrap that = (HBaseRegistryBootstrap) o;
        return bootstrapType == that.bootstrapType;
    }
}
