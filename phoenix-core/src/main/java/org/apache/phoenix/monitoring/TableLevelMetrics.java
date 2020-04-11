package org.apache.phoenix.monitoring;

import org.apache.hadoop.hbase.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.monitoring.MetricType.*;

public class TableLevelMetrics{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableLevelMetrics.class);
    private  String TableName;

    GlobalMetric selectSqlCounter = new GlobalMetricImpl(SELECT_SQL_COUNTER);
    GlobalMetric mutationBatchFailedSize = new GlobalMetricImpl(MUTATION_BATCH_FAILED_SIZE);
    GlobalMetric mutationBatchSize = new GlobalMetricImpl(MUTATION_BATCH_SIZE);
    GlobalMetric mutationBytes = new GlobalMetricImpl(MUTATION_BYTES);
    GlobalMetric mutationSqlCounter = new GlobalMetricImpl(MUTATION_SQL_COUNTER);
    GlobalMetric queryFailedCounter = new GlobalMetricImpl(QUERY_FAILED_COUNTER);
    GlobalMetric queryTimeoutCounter = new GlobalMetricImpl(QUERY_TIMEOUT_COUNTER);
    GlobalMetric numParalleScans = new GlobalMetricImpl(NUM_PARALLEL_SCANS);
    GlobalMetric numScanBytes = new GlobalMetricImpl(SCAN_BYTES);
    GlobalMetric taskExecutedCounter = new GlobalMetricImpl(TASK_EXECUTED_COUNTER);

    public TableLevelMetrics(String tableName){
        this.TableName = tableName;
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

        switch(type){
            case SELECT_SQL_COUNTER:
               selectSqlCounter.increment();
                break;
            case MUTATION_BATCH_FAILED_SIZE:
                mutationBatchFailedSize.change(val);
                break;
            case MUTATION_BATCH_SIZE:
                mutationBatchSize.change(val);
                break;
            case MUTATION_BYTES:
                mutationBytes.change(val);
                break;
            case MUTATION_SQL_COUNTER:
                 mutationSqlCounter.increment();
                 break;
            case QUERY_FAILED_COUNTER:
                queryFailedCounter.increment();
                break;
            case QUERY_TIMEOUT_COUNTER:
                queryTimeoutCounter.increment();
                break;
            case NUM_PARALLEL_SCANS:
                numParalleScans.change(val);
                break;
            case SCAN_BYTES:
                numScanBytes.change(val);
            case TASK_EXECUTED_COUNTER:
                taskExecutedCounter.increment();
            default:
                break;
        }
    }

    public void registerMetrics(MetricRegistry metricRegistry){
        metricRegistry.register(getTableName()+"_table_"+MetricType.SELECT_SQL_COUNTER, new PhoenixGlobalMetricGauge(selectSqlCounter));
        metricRegistry.register(getTableName()+"_table_"+MetricType.MUTATION_BATCH_FAILED_SIZE,  new PhoenixGlobalMetricGauge(mutationBatchFailedSize));
        metricRegistry.register(getTableName()+"_table_"+MetricType.MUTATION_BYTES,  new PhoenixGlobalMetricGauge(mutationBatchSize));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.MUTATION_BATCH_SIZE,  new PhoenixGlobalMetricGauge(mutationBytes));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.MUTATION_SQL_COUNTER,  new PhoenixGlobalMetricGauge(mutationSqlCounter));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.QUERY_FAILED_COUNTER,  new PhoenixGlobalMetricGauge(queryFailedCounter));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.QUERY_TIMEOUT_COUNTER,  new PhoenixGlobalMetricGauge(queryTimeoutCounter));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.NUM_PARALLEL_SCANS,  new PhoenixGlobalMetricGauge(numParalleScans));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.SCAN_BYTES,  new PhoenixGlobalMetricGauge(numScanBytes));
        metricRegistry.register(getTableName()+"_table_"+ MetricType.TASK_EXECUTED_COUNTER,  new PhoenixGlobalMetricGauge(taskExecutedCounter));
    }

    public Long getSelectCounter(){
        return selectSqlCounter.getValue();
    }

    public Long getMutationBatchFailedSize(){
        return mutationBatchFailedSize.getValue();
    }

    public Long getMutationBytes(){
        return mutationBytes.getValue();
    }

    public Long getMutationBatchSize(){
        return mutationBatchSize.getValue();
    }

    public Long getMutationSqlCounter(){
        return mutationSqlCounter.getValue();
    }

    public Long getQueryFailedCounter(){
        return queryFailedCounter.getValue();
    }

    public Long getQueryTimeOutCounter(){
        return queryTimeoutCounter.getValue();
    }

    public Long getNumParallelScans(){
        return numParalleScans.getValue();
    }

    public Long getScanBytes(){
        return numScanBytes.getValue();
    }

    public Long getTaskExecutedCounter(){
        return taskExecutedCounter.getValue();
    }

    public String getTableName(){
        return this.TableName;
    }

    public void removeMetricRegistry(MetricRegistry metricRegistry){
        metricRegistry.remove(getTableName()+"_table_"+MetricType.SELECT_SQL_COUNTER);
        metricRegistry.remove(getTableName()+"_table_"+MetricType.MUTATION_BATCH_FAILED_SIZE);
        metricRegistry.remove(getTableName()+"_table_"+MetricType.MUTATION_BYTES);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.MUTATION_BATCH_SIZE);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.MUTATION_SQL_COUNTER);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.QUERY_FAILED_COUNTER);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.QUERY_TIMEOUT_COUNTER);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.NUM_PARALLEL_SCANS);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.SCAN_BYTES);
        metricRegistry.remove(getTableName()+"_table_"+ MetricType.TASK_EXECUTED_COUNTER);
    }

}
