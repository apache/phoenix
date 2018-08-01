/*
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
package org.apache.phoenix.hive.mapreduce;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.hive.PhoenixSerializer;
import org.apache.phoenix.hive.PhoenixSerializer.DmlType;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.util.QueryUtil;

/**
 *
 * RecordWriter implementation. Writes records to the output
 * WARNING : There is possibility that WAL disable setting not working properly due concurrent
 * enabling/disabling WAL.
 *
 */
public class PhoenixRecordWriter<T extends DBWritable> implements RecordWriter<NullWritable, T>,
        org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter, RecordUpdater {

    private static final Log LOG = LogFactory.getLog(PhoenixRecordWriter.class);

    private Connection conn;
    private PreparedStatement pstmt;
    private long batchSize;
    private long numRecords = 0;

    private Configuration config;
    private String tableName;
    private MetaDataClient metaDataClient;
    private boolean restoreWalMode;

    // For RecordUpdater
    private long rowCountDelta = 0;
    private PhoenixSerializer phoenixSerializer;
    private ObjectInspector objInspector;
    private PreparedStatement pstmtForDelete;

    // For RecordUpdater
    public PhoenixRecordWriter(Path path, AcidOutputFormat.Options options) throws IOException {
        Configuration config = options.getConfiguration();
        Properties props = new Properties();

        try {
            initialize(config, props);
        } catch (SQLException e) {
            throw new IOException(e);
        }

        this.objInspector = options.getInspector();
        try {
            phoenixSerializer = new PhoenixSerializer(config, options.getTableProperties());
        } catch (SerDeException e) {
            throw new IOException(e);
        }
    }

    public PhoenixRecordWriter(final Configuration configuration, final Properties props) throws
            SQLException {
        initialize(configuration, props);
    }

    private void initialize(Configuration config, Properties properties) throws SQLException {
        this.config = config;
        tableName = config.get(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME);

        // Disable WAL
        String walConfigName = tableName.toLowerCase() + PhoenixStorageHandlerConstants.DISABLE_WAL;
        boolean disableWal = config.getBoolean(walConfigName, false);
        if (disableWal) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Property " + walConfigName + " is true. batch.mode will be set true. ");
            }

            properties.setProperty(PhoenixStorageHandlerConstants.BATCH_MODE, "true");
        }

        this.conn = PhoenixConnectionUtil.getInputConnection(config, properties);

        if (disableWal) {
            metaDataClient = new MetaDataClient((PhoenixConnection) conn);

            if (!PhoenixUtil.isDisabledWal(metaDataClient, tableName)) {
                // execute alter tablel statement if disable_wal is not true.
                try {
                    PhoenixUtil.alterTableForWalDisable(conn, tableName, true);
                } catch (ConcurrentTableMutationException e) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("Another mapper or task processing wal disable");
                    }
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug(tableName + "s wal disabled.");
                }

                // restore original value of disable_wal at the end.
                restoreWalMode = true;
            }
        }

        this.batchSize = PhoenixConfigurationUtil.getBatchSize(config);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Batch-size : " + batchSize);
        }

        String upsertQuery = QueryUtil.constructUpsertStatement(tableName, PhoenixUtil
                .getColumnInfoList(conn, tableName));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Upsert-query : " + upsertQuery);
        }
        this.pstmt = this.conn.prepareStatement(upsertQuery);
    }

    @Override
    public void write(NullWritable key, T record) throws IOException {
        try {
            record.write(pstmt);
            numRecords++;
            pstmt.executeUpdate();

            if (numRecords % batchSize == 0) {
                LOG.debug("Commit called on a batch of size : " + batchSize);
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException("Exception while writing to table.", e);
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        try {
            conn.commit();

            if (LOG.isInfoEnabled()) {
                LOG.info("Wrote row : " + numRecords);
            }
        } catch (SQLException e) {
            LOG.error("SQLException while performing the commit for the task.");
            throw new IOException(e);
        } finally {
            try {
                if (restoreWalMode && PhoenixUtil.isDisabledWal(metaDataClient, tableName)) {
                    try {
                        PhoenixUtil.alterTableForWalDisable(conn, tableName, false);
                    } catch (ConcurrentTableMutationException e) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Another mapper or task processing wal enable");
                        }
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(tableName + "s wal enabled.");
                    }
                }

                // flush if [table-name].auto.flush is true.
                String autoFlushConfigName = tableName.toLowerCase() +
                        PhoenixStorageHandlerConstants.AUTO_FLUSH;
                boolean autoFlush = config.getBoolean(autoFlushConfigName, false);
                if (autoFlush) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("autoFlush is true.");
                    }

                    PhoenixUtil.flush(conn, tableName);
                }

                PhoenixUtil.closeResource(pstmt);
                PhoenixUtil.closeResource(pstmtForDelete);
                PhoenixUtil.closeResource(conn);
            } catch (SQLException ex) {
                LOG.error("SQLException while closing the connection for the task.");
                throw new IOException(ex);
            }
        }
    }

    // For Testing
    public boolean isRestoreWalMode() {
        return restoreWalMode;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(Writable w) throws IOException {
        PhoenixResultWritable row = (PhoenixResultWritable) w;

        write(NullWritable.get(), (T) row);
    }

    @Override
    public void close(boolean abort) throws IOException {
        close(Reporter.NULL);
    }

    @Override
    public void insert(long currentTransaction, Object row) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("insert transaction : " + currentTransaction + ", row : " +
                    PhoenixStorageHandlerUtil.toString(row));
        }

        PhoenixResultWritable pResultWritable = (PhoenixResultWritable) phoenixSerializer
                .serialize(row, objInspector, DmlType.INSERT);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Data : " + pResultWritable.getValueList());
        }

        write(pResultWritable);
        rowCountDelta++;
    }

    @Override
    public void update(long currentTransaction, Object row) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("update transaction : " + currentTransaction + ", row : " +
                    PhoenixStorageHandlerUtil
                            .toString(row));
        }

        PhoenixResultWritable pResultWritable = (PhoenixResultWritable) phoenixSerializer
                .serialize(row, objInspector, DmlType.UPDATE);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Data : " + pResultWritable.getValueList());
        }

        write(pResultWritable);
    }

    @Override
    public void delete(long currentTransaction, Object row) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("delete transaction : " + currentTransaction + ", row : " +
                    PhoenixStorageHandlerUtil.toString(row));
        }

        PhoenixResultWritable pResultWritable = (PhoenixResultWritable) phoenixSerializer
                .serialize(row, objInspector, DmlType.DELETE);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Data : " + pResultWritable.getValueList());
        }

        if (pstmtForDelete == null) {
            try {
                String deleteQuery = PhoenixUtil.constructDeleteStatement(conn, tableName);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Delete query : " + deleteQuery);
                }

                pstmtForDelete = conn.prepareStatement(deleteQuery);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        delete(pResultWritable);

        rowCountDelta--;
    }

    private void delete(PhoenixResultWritable pResultWritable) throws IOException {
        try {
            pResultWritable.delete(pstmtForDelete);
            numRecords++;
            pstmtForDelete.executeUpdate();

            if (numRecords % batchSize == 0) {
                LOG.debug("Commit called on a batch of size : " + batchSize);
                conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException("Exception while deleting to table.", e);
        }
    }

    @Override
    public void flush() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Flush called");
        }

        try {
            conn.commit();

            if (LOG.isInfoEnabled()) {
                LOG.info("Written row : " + numRecords);
            }
        } catch (SQLException e) {
            LOG.error("SQLException while performing the commit for the task.");
            throw new IOException(e);
        }
    }

    @Override
    public SerDeStats getStats() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getStats called");
        }

        SerDeStats stats = new SerDeStats();
        stats.setRowCount(rowCountDelta);
        // Don't worry about setting raw data size diff.  There is no reasonable way  to calculate
        // that without finding the row we are updating or deleting, which would be a mess.
        return stats;
    }

    @Override
    public long getBufferedRowCount() {
        return numRecords;
    }
}
