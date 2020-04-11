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

public class GlobalPhoenixTable  {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPhoenixTable.class);
    private static final boolean isTableLevelMetricsEnabled = QueryServicesOptions.withDefaults().isTableLevelMetricsEnabled();

    private static ConcurrentMap<String,TableLevelMetrics>tablePhoenixMapping;

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

    private static GlobalPhoenixTable instance = new GlobalPhoenixTable();
    public static GlobalPhoenixTable getInstance(){
        return instance;
    }

    public void  addOrCreateTable(String tableName, MetricType type, long value){
        if(isTableLevelMetricsEnabled) {
            TableLevelMetrics tInstance = tablePhoenixMapping.get(tableName);
            if (tInstance == null) {
                tInstance = new TableLevelMetrics(tableName);
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
                TableLevelMetrics tInstance = tablePhoenixMapping.get(tableName);
                tInstance.removeMetricRegistry(metricRegistry);
                tablePhoenixMapping.remove(tableName);
            }
        }
    }

    @VisibleForTesting
    public Map<String,Long> getTableLevelMetrics(){
        Map<String,Long>TableMap = new HashMap<>();
        ConcurrentMap<String,TableLevelMetrics>map = tablePhoenixMapping;

        for(TableLevelMetrics table : map.values()) {
            TableMap.put(table.getTableName() + "_table_" + MetricType.SELECT_SQL_COUNTER, table.getSelectCounter());
            TableMap.put(table.getTableName() + "_table_" + MetricType.MUTATION_BATCH_FAILED_SIZE, table.getMutationBatchFailedSize());
            TableMap.put(table.getTableName() + "_table_" + MetricType.MUTATION_BYTES, table.getMutationBytes());
            TableMap.put(table.getTableName() + "_table_" + MetricType.MUTATION_BATCH_SIZE, table.getMutationBatchSize());
            TableMap.put(table.getTableName() + "_table_" + MetricType.MUTATION_SQL_COUNTER, table.getMutationSqlCounter());
            TableMap.put(table.getTableName() + "_table_" + MetricType.QUERY_TIMEOUT_COUNTER, table.getQueryTimeOutCounter());
            TableMap.put(table.getTableName() + "_table_" + MetricType.QUERY_FAILED_COUNTER, table.getQueryFailedCounter());
            TableMap.put(table.getTableName() + "_table_" + MetricType.NUM_PARALLEL_SCANS, table.getNumParallelScans());
            TableMap.put(table.getTableName() + "_table_" + MetricType.SCAN_BYTES, table.getScanBytes());
            TableMap.put(table.getTableName() + "_table_" + MetricType.TASK_EXECUTED_COUNTER, table.getTaskExecutedCounter());
        }
        return TableMap;
    }
}
