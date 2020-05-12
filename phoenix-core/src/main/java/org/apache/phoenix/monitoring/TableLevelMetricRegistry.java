package org.apache.phoenix.monitoring;

import org.apache.hadoop.hbase.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.monitoring.MetricType.*;

public class TableLevelMetricRegistry{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableLevelMetricRegistry.class);
    private final String tableName;
    private GlobalMetric selectSqlCounter;
    private GlobalMetric mutationBatchFailedSize;
    private GlobalMetric mutationBatchSize;
    private GlobalMetric mutationBytes;
    private GlobalMetric mutationSqlCounter;
    private GlobalMetric queryFailedCounter;
    private GlobalMetric queryTimeoutCounter;
    private GlobalMetric numParalleScans;
    private GlobalMetric numScanBytes;
    private GlobalMetric taskExecutedCounter;
    private Map<MetricType,GlobalMetric>metricRegister;

    public TableLevelMetricRegistry(final String tableName){
        this.tableName = tableName;
        this.metricRegister = new HashMap<>();
        for(MetricType type : MetricType.values()){
            switch(type) {
                case SELECT_SQL_COUNTER:
                    selectSqlCounter = new GlobalMetricImpl(SELECT_SQL_COUNTER);
                    this.metricRegister.put(SELECT_SQL_COUNTER,selectSqlCounter);
                    break;
                case MUTATION_BATCH_FAILED_SIZE:
                    this.mutationBatchFailedSize = new GlobalMetricImpl(MUTATION_BATCH_FAILED_SIZE);
                    this.metricRegister.put(MUTATION_BATCH_FAILED_SIZE,mutationBatchFailedSize);
                    break;
                case MUTATION_BATCH_SIZE:
                    this.mutationBatchSize = new GlobalMetricImpl(MUTATION_BATCH_SIZE);
                    this.metricRegister.put(MUTATION_BATCH_SIZE,mutationBatchSize);
                    break;
                case MUTATION_BYTES:
                    this.mutationBytes = new GlobalMetricImpl(MUTATION_BYTES);
                    this.metricRegister.put(MUTATION_BYTES,mutationBytes);
                    break;
                case MUTATION_SQL_COUNTER:
                    this.mutationSqlCounter = new GlobalMetricImpl(MUTATION_SQL_COUNTER);
                    this.metricRegister.put(MUTATION_SQL_COUNTER,mutationSqlCounter);
                    break;
                case QUERY_FAILED_COUNTER:
                    this.queryFailedCounter = new GlobalMetricImpl(QUERY_FAILED_COUNTER);
                    this.metricRegister.put(QUERY_FAILED_COUNTER,queryFailedCounter);
                    break;
                case QUERY_TIMEOUT_COUNTER:
                    this.queryTimeoutCounter = new GlobalMetricImpl(QUERY_TIMEOUT_COUNTER);
                    this.metricRegister.put(QUERY_TIMEOUT_COUNTER,queryTimeoutCounter);
                    break;
                case NUM_PARALLEL_SCANS:
                    this.numParalleScans = new GlobalMetricImpl(NUM_PARALLEL_SCANS);
                    this.metricRegister.put(NUM_PARALLEL_SCANS,numParalleScans);
                    break;
                case SCAN_BYTES:
                    this.numScanBytes = new GlobalMetricImpl(SCAN_BYTES);
                    this.metricRegister.put(SCAN_BYTES,numScanBytes);
                case TASK_EXECUTED_COUNTER:
                    this.taskExecutedCounter = new GlobalMetricImpl(TASK_EXECUTED_COUNTER);
                    this.metricRegister.put(TASK_EXECUTED_COUNTER,taskExecutedCounter);
                default:
                    break;
            }
        }
    }

    static class PhoenixGlobalMetricGauge implements Gauge<Long> {
        private final GlobalMetric metric;
        public PhoenixGlobalMetricGauge(GlobalMetric metric) {
            this.metric = metric;
        }

        @Override
        public Long getValue() {
            return metric.getValue();
        }
    }

    public void changeMetricValue(MetricType type, long val){
        GlobalMetric metric = metricRegister.get(type);
        if(val == 1)
            metric.increment();
        else
            metric.change(val);
    }

    public  String getMetricNameFromMetricType(MetricType type){
        return tableName + "_table_" + type;
    }

    public void registerMetrics(MetricRegistry metricRegistry){
        for(Map.Entry<MetricType,GlobalMetric>  entry : metricRegister.entrySet()){
            metricRegistry.register(getMetricNameFromMetricType(entry.getKey()), new PhoenixGlobalMetricGauge(entry.getValue()));
        }
    }

    public String getTableName(){
        return tableName;
    }

    public void removeMetricRegistry(MetricRegistry metricRegistry){
        for(Map.Entry<MetricType,GlobalMetric>  entry : metricRegister.entrySet()){
            metricRegistry.remove(getMetricNameFromMetricType(entry.getKey()));
        }
    }

    public Map<String,Long>getMetricMap(){
        Map<String,Long>map = new HashMap<>();
        for(Map.Entry<MetricType,GlobalMetric>  entry : metricRegister.entrySet()){
            map.put(getMetricNameFromMetricType(entry.getKey()),entry.getValue().getValue());
        }
        return map;
    }

}
