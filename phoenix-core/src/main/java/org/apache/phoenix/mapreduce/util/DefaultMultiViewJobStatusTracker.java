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
package org.apache.phoenix.mapreduce.util;

import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMultiViewJobStatusTracker implements MultiViewJobStatusTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMultiViewJobStatusTracker.class);

    public void updateJobStatus(PTable view, long numberOfDeletedRows, int state, Configuration config, long duration) {
        if (state == JobStatus.State.SUCCEEDED.getValue()) {
            LOGGER.debug(String.format("Number of deleted rows from view %s, TenantID %s : %d, duration : %d.",
                    view.getTableName(), view.getTenantId(), numberOfDeletedRows, duration));
        } else {
            LOGGER.debug(String.format("Job is in state %d for view %s, TenantID %s : %d.",
                    state, view.getTableName(), view.getTenantId(), numberOfDeletedRows, state));
        }
    }
}