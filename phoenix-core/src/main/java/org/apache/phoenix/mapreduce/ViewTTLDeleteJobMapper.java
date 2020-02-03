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
package org.apache.phoenix.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ViewInfoTracker;
import org.apache.phoenix.mapreduce.util.MultiViewJobStatusTracker;
import org.apache.phoenix.mapreduce.util.DefaultMultiViewJobStatusTracker;
import org.apache.phoenix.mapreduce.util.PhoenixViewTtlUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class ViewTTLDeleteJobMapper extends Mapper<NullWritable, ViewInfoTracker, NullWritable, NullWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLDeleteJobMapper.class);
    private MultiViewJobStatusTracker multiViewJobStatusTracker;

    private void initMultiViewJobStatusTracker(Configuration config) {
        try {
            Class<?> defaultViewDeletionTrackerClass = DefaultMultiViewJobStatusTracker.class;
            if (config.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_MAPPER_TRACKER_CLAZZ) != null) {
                LOGGER.info("Using customized tracker class : " + config.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_MAPPER_TRACKER_CLAZZ));
                defaultViewDeletionTrackerClass = Class.forName(
                        config.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_MAPPER_TRACKER_CLAZZ));
            } else {
                LOGGER.info("Using default tracker class ");
            }
            this.multiViewJobStatusTracker = (MultiViewJobStatusTracker) defaultViewDeletionTrackerClass.newInstance();
        } catch (Exception e) {
            LOGGER.info("exception " + e.getMessage());
            LOGGER.info("stack trace" + e.getStackTrace().toString());
        }
    }

    @Override
    protected void map(NullWritable key, ViewInfoTracker value,
                       Context context) {
        final Configuration config = context.getConfiguration();

        if (this.multiViewJobStatusTracker == null) {
            initMultiViewJobStatusTracker(config);
        }

        LOGGER.debug(String.format("Deleting from view %s, TenantID %s, and TTL value: %d",
                value.getViewName(), value.getTenantId(), value.getViewTtl()));

        try (PhoenixConnection connection =(PhoenixConnection) ConnectionUtil.getInputConnection(config) ){
            if (value.getTenantId() != null && !value.getTenantId().equals("NULL")) {
                try (PhoenixConnection tenantConnection = (PhoenixConnection)PhoenixViewTtlUtil.
                        buildTenantConnection(connection.getURL(), value.getTenantId())) {
                    deletingExpiredRows(tenantConnection, value, config);
                }
            } else {
                deletingExpiredRows(connection, value, config);
            }

        } catch (SQLException e) {
            LOGGER.error("Mapper got an exception while deleting expired rows : "
                    + e.getErrorCode() + e.getSQLState(), e.getStackTrace());
        }
    }

    private void deletingExpiredRows(PhoenixConnection connection, ViewInfoTracker value, Configuration config)
            throws SQLException {
        PTable view = PhoenixRuntime.getTable(connection, value.getViewName());

        String deleteIfExpiredStatement = "SELECT /*+ NO_INDEX */ count(*) FROM " + value.getViewName();
        deletingExpiredRows(connection, view, Long.valueOf(value.getViewTtl()),
                deleteIfExpiredStatement, config);
        List<PTable> allIndexesOnView = view.getIndexes();

        for (PTable viewIndexTable : allIndexesOnView) {
            deleteIfExpiredStatement = "SELECT /*+ INDEX */ count(*) FROM " + value.getViewName();
            deletingExpiredRows(connection, viewIndexTable, Long.valueOf(value.getViewTtl()),
                    deleteIfExpiredStatement, config);
        }
    }

    /*
     * Each Mapper that receives a MultiPhoenixViewInputSplit will execute a DeleteMutation/Scan
     *  (With DELETE_TTL_EXPIRED attribute) per view for all the views and view indexes in the split.
     * For each DeleteMutation, it bounded by the view start and stop keys for the region and
     *  TTL attributes and Delete Hint.
     */
    private void deletingExpiredRows(PhoenixConnection connection, PTable view, long viewTtl,
                                     String deleteIfExpiredStatement, Configuration config) throws SQLException {
        try {
            this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                    JobStatus.State.PREP.getValue(), config, 0);
            final PhoenixStatement statement = new PhoenixStatement(connection);

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(deleteIfExpiredStatement);
            final Scan scan = queryPlan.getContext().getScan();

            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(view);
            byte[] emptyColumnName =
                    view.getEncodingScheme() == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            view.getEncodingScheme().encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME, emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME, emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserver.DELETE_VIEW_TTL_EXPIRED, PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.MASK_VIEW_TTL_EXPIRED, PDataType.FALSE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.VIEW_TTL, Bytes.toBytes(viewTtl));

            long startTime = System.currentTimeMillis();
            this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                    JobStatus.State.RUNNING.getValue(), config, 0 );

            PhoenixResultSet rs = pstmt.newResultSet(
                    queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            pstmt.close();
            long numberOfDeletedRows = 0;
            if (rs.next()) {
                numberOfDeletedRows = rs.getLong(1);
            }
            rs.close();

            this.multiViewJobStatusTracker.updateJobStatus(view, numberOfDeletedRows,
                    JobStatus.State.SUCCEEDED.getValue(), config, System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                    JobStatus.State.FAILED.getValue(), config, 0);
            LOGGER.info("Deleting Expired Rows has an exception for : " + e.getMessage());
            throw e;
        }
    }
}
