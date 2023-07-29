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

    String CREATE_TABLE_COUNT = "createTableCount";
    String CREATE_TABLE_COUNT_DESC = "Count of CREATE TABLE DDL statements";

    String CREATE_VIEW_COUNT = "createViewCount";
    String CREATE_VIEW_COUNT_DESC = "Count of CREATE VIEW DDL statements";

    String CREATE_INDEX_COUNT = "createIndexCount";
    String CREATE_INDEX_COUNT_DESC = "Count of CREATE INDEX DDL statements";

    String CREATE_SCHEMA_COUNT = "createSchemaCount";
    String CREATE_SCHEMA_COUNT_DESC = "Count of CREATE SCHEMA DDL statements";

    String CREATE_FUNCTION_COUNT = "createFunctionCount";
    String CREATE_FUNCTION_COUNT_DESC = "Count of CREATE FUNCTION DDL statements";

    String ALTER_ADD_COLUMN_COUNT = "alterAddColumnCount";
    String ALTER_ADD_COLUMN_COUNT_DESC = "Count of ALTER statements that add columns";

    String ALTER_DROP_COLUMN_COUNT = "alterDropColumnCount";
    String ALTER_DROP_COLUMN_COUNT_DESC = "Count of ALTER statements that drop columns";

    String DROP_TABLE_COUNT = "dropTableCount";
    String DROP_TABLE_COUNT_DESC = "Count of DROP TABLE DDL statements";

    String DROP_VIEW_COUNT = "dropViewCount";
    String DROP_VIEW_COUNT_DESC = "Count of DROP VIEW DDL statements";

    String DROP_INDEX_COUNT = "dropIndexCount";
    String DROP_INDEX_COUNT_DESC = "Count of DROP INDEX DDL statements";

    String DROP_SCHEMA_COUNT = "dropSchemaCount";
    String DROP_SCHEMA_COUNT_DESC = "Count of DROP SCHEMA DDL statements";

    String DROP_FUNCTION_COUNT = "dropFunctionCount";
    String DROP_FUNCTION_COUNT_DESC = "Count of DROP FUNCTION DDL statements";

    /**
     * Updates the count of successful CREATE TABLE DDL operations
     */
    void incrementCreateTableCount();

    /**
     * Updates the count of successful CREATE VIEW DDL operations
     */
    void incrementCreateViewCount();

    /**
     * Updates the count of successful CREATE INDEX DDL operations
     */
    void incrementCreateIndexCount();

    /**
     * Updates the count of successful CREATE SCHEMA DDL operations
     */
    void incrementCreateSchemaCount();

    /**
     * Updates the count of successful CREATE FUNCTION DDL operations
     */
    void incrementCreateFunctionCount();

    /**
     * Updates the count of successful ALTER DDL operations that add columns
     */
    void incrementAlterAddColumnCount();

    /**
     * Updates the count of successful ALTER DDL operations that drop columns
     */
    void incrementAlterDropColumnCount();

    /**
     * Updates the count of successful DROP TABLE DDL operations
     */
    void incrementDropTableCount();

    /**
     * Updates the count of successful DROP VIEW DDL operations
     */
    void incrementDropViewCount();

    /**
     * Updates the count of successful DROP INDEX DDL operations
     */
    void incrementDropIndexCount();

    /**
     * Updates the count of successful DROP SCHEMA DDL operations
     */
    void incrementDropSchemaCount();

    /**
     * Updates the count of successful DROP FUNCTION DDL operations
     */
    void incrementDropFunctionCount();
}