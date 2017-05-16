/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.queryserver.metrics;


import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.queryserver.metrics.sink.PqsFileSink;
import org.apache.phoenix.queryserver.metrics.sink.PqsSink;
import org.apache.phoenix.queryserver.metrics.sink.PqsSlf4jSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.query.QueryServices.PHOENIX_PQS_FILE_SINK_FILENAME;
import static org.apache.phoenix.query.QueryServices.PHOENIX_PQS_METRIC_REPORTING_INTERVAL_MS;
import static org.apache.phoenix.query.QueryServices.PHOENIX_PQS_TYPE_OF_SINK;


public class PqsMetricsSystem {

    public final static String statementReadMetrics = "Statement-RequestReadMetrics";
    public final static String overAllReadRequestMetrics = "Statement-OverAllReadRequestMetrics";
    public final static String connectionWriteMetricsForMutations = "Connection-WriteMetricsForMutations";
    public final static String connectionReadMetricsForMutations = "Connection-ReadMetricsForMutations";

    public enum MetricType {
        global,
        request
    }

    protected static final Logger LOG = LoggerFactory.getLogger(PqsMetricsSystem.class);

    public Thread getGlobalMetricThread() {
        return globalMetricThread;
    }

    private Thread globalMetricThread = null;


    public PqsMetricsSystem(String sinkType,String fileName, Integer reportingInterval){
        PqsGlobalMetrics pqsGlobalMetricsToJMX = null;
        try {
            pqsGlobalMetricsToJMX = new PqsGlobalMetrics(sinkType, fileName,reportingInterval);
            globalMetricThread = new Thread(pqsGlobalMetricsToJMX);
            globalMetricThread.setName("globalMetricsThread");
            globalMetricThread.start();
        }catch (Exception ex){
            LOG.error(" could not instantiate the PQS Metrics System");
            if (globalMetricThread!=null) {
                try {
                    globalMetricThread.interrupt();
                } catch (Exception ine) {
                    LOG.error(" unable to interrupt the global metrics thread",ine);
                }
            }

        }
     }

    public static PqsSink getSinkObject(String typeOfSink,String filename){
        PqsSink pqsSink;
        switch(typeOfSink.toLowerCase()) {
            case "file":
                pqsSink = new PqsFileSink(filename);
                break;
            default:
                pqsSink = new PqsSlf4jSink() ;
                break;
        }
        return pqsSink;
    }

    public static String getTypeOfSink(Configuration configuration){
        return configuration.get(PHOENIX_PQS_TYPE_OF_SINK,
                QueryServicesOptions.DEFAULT_PHOENIX_PQS_TYPE_OF_SINK);
    }

    public static String getSinkFileName(Configuration configuration) {
        return configuration.get(PHOENIX_PQS_FILE_SINK_FILENAME,
                QueryServicesOptions.DEFAULT_PHOENIX_PQS_FILE_SINK_FILENAME);
    }

    public static Integer getReportingInterval(Configuration configuration) {
        return configuration.getInt(PHOENIX_PQS_METRIC_REPORTING_INTERVAL_MS,
                QueryServicesOptions.DEFAULT_PHOENIX_PQS_REPORTING_INTERVAL_MS);
    }

}

