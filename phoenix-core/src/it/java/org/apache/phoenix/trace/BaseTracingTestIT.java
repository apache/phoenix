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
package org.apache.phoenix.trace;

import static org.apache.phoenix.util.PhoenixRuntime.ANNOTATION_ATTRIB_PREFIX;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.ExposedMetricCounterLong;
import org.apache.hadoop.metrics2.impl.ExposedMetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.ExposedMetricsInfoImpl;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.trace.util.Tracing.Frequency;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;

/**
 * Base test for tracing tests - helps manage getting tracing/non-tracing
 * connections, as well as any supporting utils.
 */

public class BaseTracingTestIT extends BaseHBaseManagedTimeIT {
    @Before
    public void resetTracingTableIfExists() throws Exception {
        Connection conn = getConnectionWithoutTracing();
        conn.setAutoCommit(true);
        try {
            conn.createStatement().executeUpdate(
                    "DELETE FROM " + QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME);
        } catch (TableNotFoundException ignore) {
        }
    }

    public static Connection getConnectionWithoutTracing() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return getConnectionWithoutTracing(props);
    }

    public static Connection getConnectionWithoutTracing(Properties props) throws SQLException {
        Connection conn = getConnectionWithTracingFrequency(props, Frequency.NEVER);
        conn.setAutoCommit(false);
        return conn;
    }

    public static Connection getTracingConnection() throws Exception {
        return getTracingConnection(Collections.<String, String>emptyMap(), null);
    }

    public static Connection getTracingConnection(Map<String, String> customAnnotations,
            String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        for (Map.Entry<String, String> annot : customAnnotations.entrySet()) {
            props.put(ANNOTATION_ATTRIB_PREFIX + annot.getKey(), annot.getValue());
        }
        if (tenantId != null) {
            props.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        return getConnectionWithTracingFrequency(props, Tracing.Frequency.ALWAYS);
    }

    public static Connection getConnectionWithTracingFrequency(Properties props,
            Tracing.Frequency frequency) throws SQLException {
        Tracing.setSampling(props, frequency);
        return DriverManager.getConnection(getUrl(), props);
    }

    public static MetricsRecord createRecord(long traceid, long parentid, long spanid,
            String desc, long startTime, long endTime, String hostname, String... tags) {

        List<AbstractMetric> metrics = new ArrayList<AbstractMetric>();
        AbstractMetric span = new ExposedMetricCounterLong(asInfo(MetricInfo
                .SPAN.traceName),
                spanid);
        metrics.add(span);

        AbstractMetric parent = new ExposedMetricCounterLong(asInfo(MetricInfo.PARENT.traceName),
                parentid);
        metrics.add(parent);

        AbstractMetric start = new ExposedMetricCounterLong(asInfo(MetricInfo.START.traceName),
                startTime);
        metrics.add(start);

        AbstractMetric
                end =
                new ExposedMetricCounterLong(asInfo(MetricInfo.END.traceName), endTime);
        metrics.add(end);

        List<MetricsTag> tagsList = new ArrayList<MetricsTag>();
        int tagCount = 0;
        for (String annotation : tags) {
            MetricsTag tag =
                    new PhoenixTagImpl(MetricInfo.ANNOTATION.traceName,
                            Integer.toString(tagCount++), annotation);
            tagsList.add(tag);
        }
        String hostnameValue = "host-name.value";
        MetricsTag hostnameTag =
                new PhoenixTagImpl(MetricInfo.HOSTNAME.traceName, "", hostnameValue);
        tagsList.add(hostnameTag);

        MetricsRecord record =
                new ExposedMetricsRecordImpl(new ExposedMetricsInfoImpl(TracingUtils
                        .getTraceMetricName(traceid), desc), System.currentTimeMillis(),
                        tagsList, metrics);
        return record;
    }

    private static MetricsInfo asInfo(String name) {
        return new ExposedMetricsInfoImpl(name, "");
    }
}
