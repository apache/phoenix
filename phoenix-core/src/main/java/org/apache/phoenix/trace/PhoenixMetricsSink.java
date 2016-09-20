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
import java.util.Properties;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.metrics.Metrics;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

/**
 * Write the metrics to a phoenix table.
 * Generally, this class is instantiated via hadoop-metrics2 property files.
 * Specifically, you would create this class by adding the following to
 * by
 * This would actually be set as: <code>
 * [prefix].sink.[some instance name].class=org.apache.phoenix.trace.PhoenixMetricsSink
 * </code>, where <tt>prefix</tt> is either:
 * <ol>
 * <li>"phoenix", for the client</li>
 * <li>"hbase", for the server</li>
 * </ol>
 * and
 * <tt>some instance name</tt> is just any unique name, so properties can be differentiated if
 * there are multiple sinks of the same type created
 */
public class PhoenixMetricsSink implements MetricsSink {

    private static final Log LOG = LogFactory.getLog(PhoenixMetricsSink.class);

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

    private Connection conn;

    private String table;
    
    public PhoenixMetricsSink() {
        LOG.info("Writing tracing metrics to phoenix table");

    }

    @Override
    public void init(SubsetConfiguration config) {
        Metrics.markSinkInitialized();
        LOG.info("Phoenix tracing writer started");
    }

    /**
     * Initialize <tt>this</tt> only when we need it
     */
    private void lazyInitialize() {
        synchronized (this) {
            if (this.conn != null) {
                return;
            }
            try {
                // create the phoenix connection
                Properties props = new Properties();
                props.setProperty(QueryServices.TRACING_FREQ_ATTRIB,
                        Tracing.Frequency.NEVER.getKey());
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                Connection conn = QueryUtil.getConnectionOnServer(props, conf);
                // enable bulk loading when we have enough data
                conn.setAutoCommit(true);

                String tableName =
                        conf.get(QueryServices.TRACING_STATS_TABLE_NAME_ATTRIB,
                                QueryServicesOptions.DEFAULT_TRACING_STATS_TABLE_NAME);

                initializeInternal(conn, tableName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private void initializeInternal(Connection conn, String tableName) throws SQLException {
        this.conn = conn;
        // ensure that the target table already exists
        if (!traceTableExists(conn, tableName)) {
            createTable(conn, tableName);
        }
        this.table = tableName;
    }
    
    private boolean traceTableExists(Connection conn, String traceTableName) throws SQLException {
        try {
            PhoenixRuntime.getTable(conn, traceTableName);
            return true;
        } catch (TableNotFoundException e) {
            return false;
        }
    }
    
    /**
     * Used for <b>TESTING ONLY</b>
     * Initialize the connection and setup the table to use the
     * {@link org.apache.phoenix.query.QueryServicesOptions#DEFAULT_TRACING_STATS_TABLE_NAME}
     *
     * @param conn to store for upserts and to create the table (if necessary)
     * @param tableName TODO
     * @throws SQLException if any phoenix operation fails
     */
    @VisibleForTesting
    public void initForTesting(Connection conn, String tableName) throws SQLException {
        initializeInternal(conn, tableName);
    }

    /**
     * Create a stats table with the given name. Stores the name for use later when creating upsert
     * statements
     *
     * @param conn  connection to use when creating the table
     * @param table name of the table to create
     * @throws SQLException if any phoenix operations fails
     */
    private void createTable(Connection conn, String table) throws SQLException {
        // only primary-key columns can be marked non-null
        String ddl =
                "create table if not exists " + table + "( " +
                        TRACE.columnName + " bigint not null, " +
                        PARENT.columnName + " bigint not null, " +
                        SPAN.columnName + " bigint not null, " +
                        DESCRIPTION.columnName + " varchar, " +
                        START.columnName + " bigint, " +
                        END.columnName + " bigint, " +
                        HOSTNAME.columnName + " varchar, " +
                        TAG_COUNT + " smallint, " +
                        ANNOTATION_COUNT + " smallint" +
                        "  CONSTRAINT pk PRIMARY KEY (" + TRACE.columnName + ", "
                        + PARENT.columnName + ", " + SPAN.columnName + "))\n" +
                        // We have a config parameter that can be set so that tables are
                        // transactional by default. If that's set, we still don't want these system
                        // tables created as transactional tables, make these table non
                        // transactional
                        PhoenixDatabaseMetaData.TRANSACTIONAL + "=" + Boolean.FALSE;
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
    }

    @Override
    public void flush() {
        try {
            this.conn.commit();
        } catch (SQLException e) {
            LOG.error("Failed to commit changes to table", e);
        }
    }

    /**
     * Add a new metric record to be written.
     *
     * @param record
     */
    @Override
    public void putMetrics(MetricsRecord record) {
        // its not a tracing record, we are done. This could also be handled by filters, but safer
        // to do it here, in case it gets misconfigured
        if (!record.name().startsWith(TracingUtils.METRIC_SOURCE_KEY)) {
            return;
        }

        // don't initialize until we actually have something to write
        lazyInitialize();

        String stmt = "UPSERT INTO " + table + " (";
        // drop it into the queue of things that should be written
        List<String> keys = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        // we need to keep variable values in a separate set since they may have spaces, which
        // causes the parser to barf. Instead, we need to add them after the statement is prepared
        List<String> variableValues = new ArrayList<String>(record.tags().size());
        keys.add(TRACE.columnName);
        values.add(
                Long.parseLong(record.name().substring(TracingUtils.METRIC_SOURCE_KEY.length())));

        keys.add(DESCRIPTION.columnName);
        values.add(VARIABLE_VALUE);
        variableValues.add(record.description());

        // add each of the metrics
        for (AbstractMetric metric : record.metrics()) {
            // name of the metric is also the column name to which we write
            keys.add(MetricInfo.getColumnName(metric.name()));
            values.add(metric.value());
        }

        // get the tags out so we can set them later (otherwise, need to be a single value)
        int annotationCount = 0;
        int tagCount = 0;
        for (MetricsTag tag : record.tags()) {
            if (tag.name().equals(ANNOTATION.traceName)) {
                addDynamicEntry(keys, values, variableValues, ANNOTATION_FAMILY, tag, ANNOTATION,
                        annotationCount);
                annotationCount++;
            } else if (tag.name().equals(TAG.traceName)) {
                addDynamicEntry(keys, values, variableValues, TAG_FAMILY, tag, TAG, tagCount);
                tagCount++;
            } else if (tag.name().equals(HOSTNAME.traceName)) {
                keys.add(HOSTNAME.columnName);
                values.add(VARIABLE_VALUE);
                variableValues.add(tag.value());
            } else if (tag.name().equals("Context")) {
                // ignored
            } else {
                LOG.error("Got an unexpected tag: " + tag);
            }
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
            // Not going through the standard route of using statement.execute() as that code path
            // is blocked if the metadata hasn't been been upgraded to the new minor release. 
            MutationPlan plan = ps.unwrap(PhoenixPreparedStatement.class).compileMutation(stmt);
            MutationState state = conn.unwrap(PhoenixConnection.class).getMutationState();
            MutationState newState = plan.execute();
            state.join(newState);
        } catch (SQLException e) {
            LOG.error("Could not write metric: \n" + record + " to prepared statement:\n" + stmt,
                    e);
        }
    }

    public static String getDynamicColumnName(String family, String column, int count) {
        return COLUMN_JOIN.join(family, column) + count;
    }

    private void addDynamicEntry(List<String> keys, List<Object> values,
            List<String> variableValues, String family, MetricsTag tag,
            MetricInfo metric, int count) {
        // <family><.dynColumn><count> <VARCHAR>
        keys.add(getDynamicColumnName(family, metric.columnName, count) + " VARCHAR");

        // build the annotation value
        String val = tag.description() + " - " + tag.value();
        values.add(VARIABLE_VALUE);
        variableValues.add(val);
    }

    @VisibleForTesting
    public void clearForTesting() throws SQLException {
        this.conn.rollback();
    }
}