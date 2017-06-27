/**
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
package org.apache.phoenix.trace;

import static org.apache.phoenix.metrics.MetricInfo.ANNOTATION;
import static org.apache.phoenix.metrics.MetricInfo.DESCRIPTION;
import static org.apache.phoenix.metrics.MetricInfo.END;
import static org.apache.phoenix.metrics.MetricInfo.HOSTNAME;
import static org.apache.phoenix.metrics.MetricInfo.PARENT;
import static org.apache.phoenix.metrics.MetricInfo.SPAN;
import static org.apache.phoenix.metrics.MetricInfo.START;
import static org.apache.phoenix.metrics.MetricInfo.TAG;
import static org.apache.phoenix.metrics.MetricInfo.TRACE;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.htrace.Span;
import org.apache.htrace.TimelineAnnotation;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Sink for the trace spans pushed into the queue by {@link TraceSpanReceiver}. The class
 * instantiates a thread pool of configurable size, which will pull the data from queue and write to
 * the Phoenix Trace Table in batches. Various configuration options include thread pool size and
 * batch commit size.
 */
public class TraceWriter {
    private static final Log LOG = LogFactory.getLog(TraceWriter.class);

    private static final String VARIABLE_VALUE = "?";

    private static final Joiner COLUMN_JOIN = Joiner.on(".");
    static final String TAG_FAMILY = "tags";
    /**
     * Count of the number of tags we are storing for this row
     */
    static final String TAG_COUNT = COLUMN_JOIN.join(TAG_FAMILY, "count");

    static final String ANNOTATION_FAMILY = "annotations";
    static final String ANNOTATION_COUNT = COLUMN_JOIN.join(ANNOTATION_FAMILY, "count");

    /**
     * Join strings on a comma
     */
    private static final Joiner COMMAS = Joiner.on(',');

    private String tableName;
    private int batchSize;
    private int numThreads;
    private TraceSpanReceiver traceSpanReceiver;

    protected ScheduledExecutorService executor;

    public TraceWriter(String tableName, int numThreads, int batchSize) {

        this.batchSize = batchSize;
        this.numThreads = numThreads;
        this.tableName = tableName;
    }

    public void start() {

        traceSpanReceiver = getTraceSpanReceiver();
        if (traceSpanReceiver == null) {
            LOG.warn(
                "No receiver has been initialized for TraceWriter. Traces will not be written.");
            LOG.warn("Restart Phoenix to try again.");
            return;
        }

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setDaemon(true).setNameFormat("PHOENIX-METRICS-WRITER");
        executor = Executors.newScheduledThreadPool(this.numThreads, builder.build());

        for (int i = 0; i < this.numThreads; i++) {
            executor.scheduleAtFixedRate(new FlushMetrics(), 0, 10, TimeUnit.SECONDS);
        }

        LOG.info("Writing tracing metrics to phoenix table");
    }

    @VisibleForTesting
    protected TraceSpanReceiver getTraceSpanReceiver() {
        return Tracing.getTraceSpanReceiver();
    }

    public class FlushMetrics implements Runnable {

        private Connection conn;
        private int counter = 0;

        public FlushMetrics() {
            conn = getConnection(tableName);
        }

        @Override
        public void run() {
            if (conn == null) return;
            while (!traceSpanReceiver.isSpanAvailable()) {
                Span span = traceSpanReceiver.getSpan();
                if (null == span) break;
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Span received: " + span.toJson());
                }
                addToBatch(span);
                counter++;
                if (counter >= batchSize) {
                    commitBatch(conn);
                    counter = 0;
                }
            }
        }

        private void addToBatch(Span span) {

            String stmt = "UPSERT INTO " + tableName + " (";
            // drop it into the queue of things that should be written
            List<String> keys = new ArrayList<String>();
            List<Object> values = new ArrayList<Object>();
            // we need to keep variable values in a separate set since they may have spaces, which
            // causes the parser to barf. Instead, we need to add them after the statement is
            // prepared
            List<String> variableValues = new ArrayList<String>();
            keys.add(TRACE.columnName);
            values.add(span.getTraceId());

            keys.add(DESCRIPTION.columnName);
            values.add(VARIABLE_VALUE);
            variableValues.add(span.getDescription());

            keys.add(SPAN.traceName);
            values.add(span.getSpanId());

            keys.add(PARENT.traceName);
            values.add(span.getParentId());

            keys.add(START.traceName);
            values.add(span.getStartTimeMillis());

            keys.add(END.traceName);
            values.add(span.getStopTimeMillis());

            int annotationCount = 0;
            int tagCount = 0;

            // add the tags to the span. They were written in order received so we mark them as such
            for (TimelineAnnotation ta : span.getTimelineAnnotations()) {
                addDynamicEntry(keys, values, variableValues, TAG_FAMILY,
                    Long.toString(ta.getTime()), ta.getMessage(), TAG, tagCount);
                tagCount++;
            }

            // add the annotations. We assume they are serialized as strings and integers, but that
            // can
            // change in the future
            Map<byte[], byte[]> annotations = span.getKVAnnotations();
            for (Map.Entry<byte[], byte[]> annotation : annotations.entrySet()) {
                Pair<String, String> val =
                        TracingUtils.readAnnotation(annotation.getKey(), annotation.getValue());
                addDynamicEntry(keys, values, variableValues, ANNOTATION_FAMILY, val.getFirst(),
                    val.getSecond(), ANNOTATION, annotationCount);
                annotationCount++;
            }

            // add the tag count, now that we know it
            keys.add(TAG_COUNT);
            // ignore the hostname in the tags, if we know it
            values.add(tagCount);

            keys.add(ANNOTATION_COUNT);
            values.add(annotationCount);

            // compile the statement together
            stmt += COMMAS.join(keys);
            stmt += ") VALUES (" + COMMAS.join(values) + ")";

            if (LOG.isTraceEnabled()) {
                LOG.trace("Logging metrics to phoenix table via: " + stmt);
                LOG.trace("With tags: " + variableValues);
            }
            try {
                PreparedStatement ps = conn.prepareStatement(stmt);
                // add everything that wouldn't/may not parse
                int index = 1;
                for (String tag : variableValues) {
                    ps.setString(index++, tag);
                }

                // Not going through the standard route of using statement.execute() as that code
                // path
                // is blocked if the metadata hasn't been been upgraded to the new minor release.
                MutationPlan plan = ps.unwrap(PhoenixPreparedStatement.class).compileMutation(stmt);
                MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
                MutationState newState = plan.execute();
                state.join(newState);
            } catch (SQLException e) {
                LOG.error("Could not write metric: \n" + span + " to prepared statement:\n" + stmt,
                    e);
            }
        }
    }

    public static String getDynamicColumnName(String family, String column, int count) {
        return COLUMN_JOIN.join(family, column) + count;
    }

    private void addDynamicEntry(List<String> keys, List<Object> values,
            List<String> variableValues, String family, String desc, String value,
            MetricInfo metric, int count) {
        // <family><.dynColumn><count> <VARCHAR>
        keys.add(getDynamicColumnName(family, metric.columnName, count) + " VARCHAR");

        // build the annotation value
        String val = desc + " - " + value;
        values.add(VARIABLE_VALUE);
        variableValues.add(val);
    }

    protected Connection getConnection(String tableName) {

        try {
            // create the phoenix connection
            Properties props = new Properties();
            props.setProperty(QueryServices.TRACING_FREQ_ATTRIB, Tracing.Frequency.NEVER.getKey());
            Configuration conf = HBaseConfiguration.create();
            Connection conn = QueryUtil.getConnectionOnServer(props, conf);

            if (!traceTableExists(conn, tableName)) {
                createTable(conn, tableName);
            }

            LOG.info(
                "Created new connection for tracing " + conn.toString() + " Table: " + tableName);
            return conn;
        } catch (Exception e) {
            LOG.error("Tracing will NOT be pursued. New connection failed for tracing Table: "
                    + tableName,
                e);
            LOG.error("Restart Phoenix to retry.");
            return null;
        }
    }

    protected boolean traceTableExists(Connection conn, String traceTableName) throws SQLException {
        try {
            PhoenixRuntime.getTable(conn, traceTableName);
            return true;
        } catch (TableNotFoundException e) {
            return false;
        }
    }

    /**
     * Create a stats table with the given name. Stores the name for use later when creating upsert
     * statements
     * @param conn connection to use when creating the table
     * @param table name of the table to create
     * @throws SQLException if any phoenix operations fails
     */
    protected void createTable(Connection conn, String table) throws SQLException {
        // only primary-key columns can be marked non-null
        String ddl =
                "create table if not exists " + table + "( " + TRACE.columnName
                        + " bigint not null, " + PARENT.columnName + " bigint not null, "
                        + SPAN.columnName + " bigint not null, " + DESCRIPTION.columnName
                        + " varchar, " + START.columnName + " bigint, " + END.columnName
                        + " bigint, " + HOSTNAME.columnName + " varchar, " + TAG_COUNT
                        + " smallint, " + ANNOTATION_COUNT + " smallint"
                        + "  CONSTRAINT pk PRIMARY KEY (" + TRACE.columnName + ", "
                        + PARENT.columnName + ", " + SPAN.columnName + "))\n" +
                        // We have a config parameter that can be set so that tables are
                        // transactional by default. If that's set, we still don't want these system
                        // tables created as transactional tables, make these table non
                        // transactional
                        PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
    }

    protected void commitBatch(Connection conn) {
        try {
            conn.commit();
        } catch (SQLException e) {
            LOG.error(
                "Unable to commit traces on conn: " + conn.toString() + " to table: " + tableName,
                e);
        }
    }

}
