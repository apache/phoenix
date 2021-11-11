/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.schema.metrics;

public interface MetricsMetadataSource {
    // Metrics2 and JMX constants
    String METRICS_NAME = "PhoenixMetadata";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about the Phoenix MetadataEndpoint";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String CREATE_EXPORT_COUNT = "createExportCount";
    String CREATE_EXPORT_COUNT_DESC = "Count of CREATE DDL statements exported to schema registry";

    String CREATE_EXPORT_FAILURE_COUNT = "createExportFailureCount";
    String CREATE_EXPORT_FAILURE_COUNT_DESC = "Count of create DDL that failed on export "
        + "to schema registry";

    String CREATE_EXPORT_TIME = "createExportTime";
    String CREATE_EXPORT_TIME_DESC = "Time taken while exporting CREATE DDL statements to schema registry";

    String CREATE_EXPORT_FAILURE_TIME = "createExportFailureTime";
    String CREATE_EXPORT_FAILURE_TIME_DESC = "Time taken while failing to export "
        + "CREATE DDL to schema registry";

    String ALTER_EXPORT_COUNT = "alterExportCount";
    String ALTER_EXPORT_COUNT_DESC = "Count of ALTER DDL statements exported to schema registry";

    String ALTER_EXPORT_FAILURE_COUNT = "alterExportFailureCount";
    String ALTER_EXPORT_FAILURE_COUNT_DESC = "Count of ALTER DDL that failed on export "
        + "to schema registry";

    String ALTER_EXPORT_TIME = "alterExportTime";
    String ALTER_EXPORT_TIME_DESC = "Time taken while exporting ALTER DDL statements to schema registry";

    String ALTER_EXPORT_FAILURE_TIME = "alterExportFailureTime";
    String ALTER_EXPORT_FAILURE_TIME_DESC = "Time taken while failing to export "
        + "ALTER DDL to schema registry";

    /**
     * Updates the count of successful requests to the schema registry for CREATE statements
     */
    void incrementCreateExportCount();

    /**
     * Updates the histogram of time taken to update the schema registry for CREATE statements
     * @param t Time taken
     */
    void updateCreateExportTime(long t);

    /**
     * Updates the count of unsuccessful requests to the schema registry for CREATE statements
     */
    void incrementCreateExportFailureCount();

    /**
     * Updates the histogram of time taken trying and failing to
     * update the schema registry for CREATE statements
     * @param t time taken
     */
    void updateCreateExportFailureTime(long t);

    /**
     * Updates the count of successful requests to the schema registry for ALTER statements
     */
    void incrementAlterExportCount();

    /**
     * Updates the histogram of time taken updating the schema registry for ALTER statements
     * @param t time takne
     */
    void updateAlterExportTime(long t);

    /**
     * Updates the count of unsuccessful requests to the schema registry for ALTER statements
     */
    void incrementAlterExportFailureCount();

    /**
     * Updates the histogram of time taken trying and failing to update the schema registry for
     * ALTER statements
     * @param t time taken
     */
    void updateAlterExportFailureTime(long t);
}
