package org.apache.phoenix.coprocessor.metrics;

public class MetricsPhoenixCoprocessorSourceFactory {

    private static final MetricsPhoenixCoprocessorSourceFactory
            INSTANCE = new MetricsPhoenixCoprocessorSourceFactory();
    private MetricsPhoenixTTLSource phoenixTTLSource;

    public static MetricsPhoenixCoprocessorSourceFactory getInstance() {
        return INSTANCE;
    }

    public synchronized MetricsPhoenixTTLSource getPhoenixTTLSource() {
        if (INSTANCE.phoenixTTLSource == null) {
            INSTANCE.phoenixTTLSource = new MetricsPhoenixTTLSourceImpl();
        }
        return INSTANCE.phoenixTTLSource;
    }

}
