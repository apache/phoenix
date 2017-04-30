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
import org.apache.hadoop.hbase.HBaseConfiguration;

public class PqsConfiguration {

    private static final Configuration configuration= HBaseConfiguration.create();
    public static final String PHOENIX_QUERY_SERVER_METRICS = "phoenix.query.server.metrics";
    public static final String PHOENIX_PQS_METRIC_REPORTING_INTERVAL_MS = "phoenix.query.server.metrics.report.interval.ms";
    public static final String PHOENIX_PQS_TYPE_OF_SINK = "phoenix.query.server.metrics.type.of.pqsSink";
    public static final String PHOENIX_PQS_FILE_SINK_FILENAME = "phoenix.query.server.file.pqsSink.filename";

    public enum typeOfSink {
        file,
        slf4j
    }

    public static final Integer DEFAULT_PHOENIX_PQS_REPORTING_INTERVAL_MS = 360;
    public static final typeOfSink DEFAULT_PHOENIX_PQS_TYPE_OF_SINK = typeOfSink.slf4j;
    public static final boolean DEFAULT_PHOENIX_QUERY_SERVER_METRICS = true;
    public static final String DEFAULT_PHOENIX_PQS_FILE_SINK_FILENAME = "filename";


    public static int getReportingInterval(){
        return configuration.getInt(PHOENIX_PQS_METRIC_REPORTING_INTERVAL_MS,DEFAULT_PHOENIX_PQS_REPORTING_INTERVAL_MS);
    }

    public static String getTypeOfSink(){
        return configuration.get(PHOENIX_PQS_TYPE_OF_SINK,DEFAULT_PHOENIX_PQS_TYPE_OF_SINK.name());
    }

    public static boolean isMetricsTurnedOn(){
        return configuration.getBoolean(PHOENIX_QUERY_SERVER_METRICS,DEFAULT_PHOENIX_QUERY_SERVER_METRICS);
    }

    public static String getFileSinkFilename(){
        return configuration.get(PHOENIX_PQS_FILE_SINK_FILENAME,DEFAULT_PHOENIX_PQS_FILE_SINK_FILENAME);
    }

}
