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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable.ViewInfoJobState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMultiViewJobStatusTracker implements MultiViewJobStatusTracker {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DefaultMultiViewJobStatusTracker.class);

    public void updateJobStatus(ViewInfoTracker view, long numberOfDeletedRows, int state,
                                Configuration config, long duration, String mrJobName) {
        if (state == ViewInfoJobState.SUCCEEDED.getValue()) {
            LOGGER.debug(String.format("Number of deleted rows from view %s, TenantID %s, " +
                            "and Source Table Name %s : " +
                            "number of deleted row %d, duration : %d, mr job name : %s.",
                    view.getViewName(), view.getTenantId(), view.getRelationName(),
                    numberOfDeletedRows, duration, mrJobName));
        } else if (state == ViewInfoJobState.DELETED.getValue()) {
            LOGGER.debug(String.format("View has been deleted, view info : view %s, TenantID %s, " +
                            "and Source Table Name %s : %d," +
                            " mr job name : %s.", view.getViewName(), view.getTenantId(),
                    view.getRelationName(), mrJobName));
        } else {
            LOGGER.debug(String.format("Job is in state %d for view %s, TenantID %s, " +
                            "Source Table Name %s , and duration : %d, " +
                            "mr job name : %s.", state, view.getViewName(), view.getTenantId(),
                    view.getRelationName(), duration, mrJobName));
        }
    }
}