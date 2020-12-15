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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ViewInfoTracker;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable.ViewInfoJobState;
import org.apache.phoenix.mapreduce.util.MultiViewJobStatusTracker;
import org.apache.phoenix.mapreduce.util.DefaultMultiViewJobStatusTracker;
import org.apache.phoenix.mapreduce.util.PhoenixMultiInputUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class ViewTTLDeleteJobMapper extends Mapper<NullWritable, ViewInfoTracker, NullWritable, NullWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewTTLDeleteJobMapper.class);
    private MultiViewJobStatusTracker multiViewJobStatusTracker;

    private void initMultiViewJobStatusTracker(Configuration config) throws Exception {
        try {
            Class<?> defaultViewDeletionTrackerClass = DefaultMultiViewJobStatusTracker.class;
            if (config.get(PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ) != null) {
                LOGGER.info("Using customized tracker class : " +
                        config.get(PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ));
                defaultViewDeletionTrackerClass = Class.forName(
                        config.get(PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_TRACKER_CLAZZ));
            } else {
                LOGGER.info("Using default tracker class ");
            }
            this.multiViewJobStatusTracker = (MultiViewJobStatusTracker) defaultViewDeletionTrackerClass.newInstance();
        } catch (Exception e) {
            LOGGER.error("Getting exception While initializing initMultiViewJobStatusTracker with error message");
            LOGGER.error("stack trace" + e.getStackTrace().toString());
            throw e;
        }
    }

    @Override
    protected void map(NullWritable key, ViewInfoTracker value, Context context) throws IOException  {
        try {
            final Configuration config = context.getConfiguration();

            if (this.multiViewJobStatusTracker == null) {
                initMultiViewJobStatusTracker(config);
            }

            LOGGER.debug(String.format("Deleting from view %s, TenantID %s, and TTL value: %d",
                    value.getViewName(), value.getTenantId(), value.getViewTtl()));

            try (PhoenixConnection connection = (PhoenixConnection) ConnectionUtil.getInputConnection(config)) {
                if (value.getTenantId() != null && !value.getTenantId().equals("NULL")) {
                    try (PhoenixConnection tenantConnection = (PhoenixConnection) PhoenixMultiInputUtil.
                            buildTenantConnection(connection.getURL(), value.getTenantId())) {
                        deletingExpiredRows(tenantConnection, value, config);
                    }
                } else {
                    deletingExpiredRows(connection, value, config);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Mapper got an exception while deleting expired rows : " + e.getMessage() );
            throw new IOException(e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOGGER.error("Getting IOException while running View TTL Deletion Job mapper with error : "
                    + e.getMessage());
            throw new IOException(e.getMessage(), e.getCause());
        }
    }

    private void deletingExpiredRows(PhoenixConnection connection, ViewInfoTracker view, Configuration config)
            throws SQLException {

        String deleteIfExpiredStatement = "DELETE FROM " + view.getViewName() +
                " WHERE TO_NUMBER(NOW()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP()) > " + view.getViewTtl();

        try {
            this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                    ViewInfoJobState.PREP.getValue(), config, 0);

            long startTime = System.currentTimeMillis();
            this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                    ViewInfoJobState.RUNNING.getValue(), config, 0 );
            connection.setAutoCommit(true);
            int numberOfDeletedRows = connection.createStatement().executeUpdate(deleteIfExpiredStatement);

            this.multiViewJobStatusTracker.updateJobStatus(view, numberOfDeletedRows,
                    ViewInfoJobState.SUCCEEDED.getValue(), config, System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            String viewInfo = view.getTenantId() == null ? view.getViewName() : view.getTenantId() + "." + view.getViewName();

            if (e instanceof SQLException && ((SQLException) e).getErrorCode() == SQLExceptionCode.TABLE_UNDEFINED.getErrorCode()) {
                LOGGER.info(viewInfo + " has been deleted : " + e.getMessage());
                this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                        ViewInfoJobState.DELETED.getValue(), config, 0);
            } else {
                LOGGER.error("Deleting " + viewInfo + " expired rows has an exception for : " + e.getMessage());
                this.multiViewJobStatusTracker.updateJobStatus(view, 0,
                        ViewInfoJobState.FAILED.getValue(), config, 0);
                throw e;
            }

        }
    }
}