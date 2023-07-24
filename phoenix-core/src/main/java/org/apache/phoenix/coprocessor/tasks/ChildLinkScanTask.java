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
package org.apache.phoenix.coprocessor.tasks;

import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

/*
Task to run a simple select * query on SYSTEM.CHILD_LINK table
to trigger read repair and verify any unverified rows.
 */
public class ChildLinkScanTask extends BaseTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChildLinkScanTask.class);
    private static final String CHILD_LINK_QUERY = "SELECT COUNT(*) FROM SYSTEM.CHILD_LINK";
    private static boolean isDisabled = false;

    @VisibleForTesting
    public static void disableChildLinkScanTask(boolean disable) {
        isDisabled = disable;
    }

    @Override
    public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {

        if (isDisabled) {
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
                    "ChildLinkScan task is disabled.");
        }

        int count = 0;
        try {
            PhoenixConnection pconn = QueryUtil.getConnectionOnServer(
                    env.getConfiguration()).unwrap(PhoenixConnection.class);
            ResultSet rs = pconn.createStatement().executeQuery(CHILD_LINK_QUERY);
            if (rs.next()) {
                count = rs.getInt(1);
            }
        }
        catch (Exception e) {
            LOGGER.error("Exception in Child Link Scan Task: " + e);
            return new TaskRegionObserver.TaskResult(
                    TaskRegionObserver.TaskResultCode.FAIL, e.getMessage());
        }
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS,
                                            "Number of rows in SYSTEM.CHILD_LINK: " + count);
    }

    @Override
    public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord)
            throws Exception {
        return null;
    }
}
