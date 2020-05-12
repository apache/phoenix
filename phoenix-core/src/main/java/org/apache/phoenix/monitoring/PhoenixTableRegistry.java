package org.apache.phoenix.monitoring;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Central place where we keep track of all the Table Level phoenix metrics.
 * Registers as new MetricSource with Hbase Registry.
 * Register each tableMetrics and store the instance of it associated with TableName in a map
 */

public class PhoenixTableRegistry  {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixTableRegistry.class);
    private static final boolean isTableLevelMetricsEnabled = QueryServicesOptions.withDefaults().isTableLevelMetricsEnabled();

    private static ConcurrentMap<String,TableLevelMetricRegistry>tablePhoenixMapping;

    static MetricRegistry metricRegistry;
    static {
        if(isTableLevelMetricsEnabled) {
            metricRegistry = createMetricRegistry();
            GlobalMetricRegistriesAdapter.getInstance().registerMetricRegistry(metricRegistry);
            tablePhoenixMapping = new ConcurrentHashMap<>();
        }
    }

    private static MetricRegistry createMetricRegistry() {
        LOGGER.info("Creating Metric Registry for Phoenix Table Level Metrics");
        MetricRegistryInfo registryInfo = new MetricRegistryInfo("PHOENIX-TableLevel", "Phoenix Client Metrics",
                "phoenixTableLevel", "Phoenix,sub=CLIENT", true);
        return MetricRegistries.global().create(registryInfo);
    }

    private static PhoenixTableRegistry instance = new PhoenixTableRegistry();
    public static PhoenixTableRegistry getInstance(){
        return instance;
    }

    public void  addOrCreateTable(String tableName, MetricType type, long value){
        if(isTableLevelMetricsEnabled) {
            TableLevelMetricRegistry tInstance = tablePhoenixMapping.get(tableName);
            if (tInstance == null) {
                tInstance = new TableLevelMetricRegistry(tableName);
                tInstance.registerMetrics(metricRegistry);
                tablePhoenixMapping.put(tableName, tInstance);
            }
            tInstance.changeMetricValue(type, value);
        }
    }

    //will be enabled in code flow in sub-sequent Iterations.
    public void deleteTable(String tableName){
        if(isTableLevelMetricsEnabled) {
            if(tablePhoenixMapping.containsKey(tableName)){
                TableLevelMetricRegistry tInstance = tablePhoenixMapping.get(tableName);
                tInstance.removeMetricRegistry(metricRegistry);
                tablePhoenixMapping.remove(tableName);
            }
        }
    }

    @VisibleForTesting
    public Map<String,Long> getTableLevelMetrics(){
        Map<String,Long>tableMap = new HashMap<>();
        ConcurrentMap<String,TableLevelMetricRegistry>map = tablePhoenixMapping;
        for(TableLevelMetricRegistry table : map.values()) {
            tableMap.putAll(table.getMetricMap());
        }
        return tableMap;
    }

    @VisibleForTesting
    public void clearTableLevelMetrics(){
        tablePhoenixMapping.clear();
    }
}
