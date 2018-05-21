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
package org.apache.phoenix.hive;

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
import org.apache.phoenix.hive.PhoenixSerializer.DmlType;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixResultWritable;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.util.QueryUtil;

public class PhoenixRecordUpdater implements RecordUpdater {

    private static final Log LOG = LogFactory.getLog(PhoenixRecordUpdater.class);

    private final Connection conn;
    private final PreparedStatement pstmt;
    private final long batchSize;
    private long numRecords = 0;

    private Configuration config;
    private String tableName;
    private MetaDataClient metaDataClient;
    private boolean restoreWalMode;

    private long rowCountDelta = 0;

    private PhoenixSerializer phoenixSerializer;
    private ObjectInspector objInspector;
    private PreparedStatement pstmtForDelete;

    public PhoenixRecordUpdater(Path path, AcidOutputFormat.Options options) throws IOException {
        this.config = options.getConfiguration();
        tableName = config.get(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME);

        Properties props = new Properties();

        try {
            // Disable WAL
            String walConfigName = tableName.toLowerCase() + PhoenixStorageHandlerConstants
                    .DISABLE_WAL;
            boolean disableWal = config.getBoolean(walConfigName, false);
            if (disableWal) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(walConfigName + " is true. batch.mode will be set true.");
                }

                props.setProperty(PhoenixStorageHandlerConstants.BATCH_MODE, "true");
            }

            this.conn = PhoenixConnectionUtil.getInputConnection(config, props);

            if (disableWal) {
                metaDataClient = new MetaDataClient((PhoenixConnection) conn);

                if (!PhoenixUtil.isDisabledWal(metaDataClient, tableName)) {
                    // execute alter tablel statement if disable_wal is not true.
                    try {
                        PhoenixUtil.alterTableForWalDisable(conn, tableName, true);
                    } catch (ConcurrentTableMutationException e) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Concurrent modification of disableWAL");
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

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.io.RecordUpdater#insert(long, java.lang.Object)
     */
    @Override
    public void insert(long currentTransaction, Object row) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Insert - currentTranscation : " + currentTransaction + ", row : " +
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

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.io.RecordUpdater#update(long, java.lang.Object)
     */
    @Override
    public void update(long currentTransaction, Object row) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Update - currentTranscation : " + currentTransaction + ", row : " +
                    PhoenixStorageHandlerUtil.toString(row));
        }

        PhoenixResultWritable pResultWritable = (PhoenixResultWritable) phoenixSerializer
                .serialize(row, objInspector, DmlType.UPDATE);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Data : " + pResultWritable.getValueList());
        }

        write(pResultWritable);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.io.RecordUpdater#delete(long, java.lang.Object)
     */
    @Override
    public void delete(long currentTransaction, Object row) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Delete - currentTranscation : " + currentTransaction + ", row : " +
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

    private void write(PhoenixResultWritable pResultWritable) throws IOException {
        try {
            pResultWritable.write(pstmt);
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

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.io.RecordUpdater#flush()
     */
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

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.io.RecordUpdater#close(boolean)
     */
    @Override
    public void close(boolean abort) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("abort : " + abort);
        }

        try {
            conn.commit();

            if (LOG.isInfoEnabled()) {
                LOG.info("Written row : " + numRecords);
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
                            LOG.warn("Concurrent modification of disableWAL");
                        }
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(tableName + "s wal enabled.");
                    }
                }

                // flush when [table-name].auto.flush is true.
                String autoFlushConfigName = tableName.toLowerCase() +
                        PhoenixStorageHandlerConstants.AUTO_FLUSH;
                boolean autoFlush = config.getBoolean(autoFlushConfigName, false);
                if (autoFlush) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("autoFlush is " + autoFlush);
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

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.io.RecordUpdater#getStats()
     */
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
