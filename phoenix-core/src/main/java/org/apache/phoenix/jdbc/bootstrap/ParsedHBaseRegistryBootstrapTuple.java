package org.apache.phoenix.jdbc.bootstrap;

public class ParsedHBaseRegistryBootstrapTuple {
    public HBaseRegistryBootstrap getBootstrap() {
        return bootstrap;
    }

    private final HBaseRegistryBootstrap bootstrap;

    public String getRemainingURL() {
        return remainingURL;
    }

    private final String remainingURL;

    public ParsedHBaseRegistryBootstrapTuple(HBaseRegistryBootstrap bootstrap, String remainingURL){
        this.bootstrap = bootstrap;
        this.remainingURL = remainingURL;
    }
}
