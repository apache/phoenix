/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.metrics;

/**
 * Metrics and their conversion from the trace name to the name we store in the stats table
 */
public enum MetricInfo {

    TRACE("", "trace_id"),
    SPAN("span_id", "span_id"),
    PARENT("parent_id", "parent_id"),
    START("start_time", "start_time"),
    END("end_time", "end_time"),
    TAG("phoenix.tag", "t"),
    ANNOTATION("phoenix.annotation", "a"),
    HOSTNAME("Hostname", "hostname"),
    DESCRIPTION("", "description");

    public final String traceName;
    public final String columnName;

    private MetricInfo(String traceName, String columnName) {
        this.traceName = traceName;
        this.columnName = columnName;
    }

    public static String getColumnName(String traceName) {
        for (MetricInfo info : MetricInfo.values()) {
            if (info.traceName.equals(traceName)) {
                return info.columnName;
            }
        }
        throw new IllegalArgumentException("Unknown tracename: " + traceName);
    }
}
