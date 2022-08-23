package org.apache.phoenix.jdbc.bootstrap;

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;

import java.util.Map;

public abstract class HBaseRegistryBootstrap {

    private final EmbeddedDriverContext embeddedDriverContext;

    public HBaseRegistryBootstrap(EmbeddedDriverContext edc) {
        this.embeddedDriverContext = edc;
    }

    public EmbeddedDriverContext getEmbeddedDriverContext() {
        return this.embeddedDriverContext;
    }

    public abstract HBaseRegistryBootstrap normalize();

    public abstract HBaseRegistryBootstrapType getBootstrapType();

    public abstract Map<String, String> generateConnectionProps(EmbeddedDriverContext edc);

    public String getStringForConnectionString() {
        final String rtn = this.toString();
        if (!Strings.isNullOrEmpty(rtn)) {
            return "+" + rtn.toLowerCase();
        }

        return "";
    }

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
