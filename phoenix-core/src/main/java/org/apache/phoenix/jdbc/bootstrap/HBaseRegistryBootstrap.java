package org.apache.phoenix.jdbc.bootstrap;

import java.util.Map;

public abstract class HBaseRegistryBootstrap {

    public abstract HBaseRegistryBootstrap normalize();

    public abstract HBaseRegistryBootstrapType getBootstrapType();

    public abstract Map<String, String> generateConnectionProps(String quorum, Integer port, String rootNode);

    @Override
    public String toString() {
        return getBootstrapType().toString();
    }

    @Override
    public int hashCode() {
        return getBootstrapType().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HBaseRegistryBootstrap that = (HBaseRegistryBootstrap) o;
        return getBootstrapType() == that.getBootstrapType();
    }
}
