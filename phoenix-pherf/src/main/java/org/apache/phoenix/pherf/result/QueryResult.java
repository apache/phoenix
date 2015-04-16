/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.result;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.phoenix.pherf.PherfConstants.RunMode;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.util.DateUtil;

public class QueryResult extends Query {
    private List<ThreadTime> threadTimes = new ArrayList<ThreadTime>();

    public synchronized List<ThreadTime> getThreadTimes() {
        return this.threadTimes;
    }

    public synchronized void setThreadTimes(List<ThreadTime> threadTimes) {
        this.threadTimes = threadTimes;
    }

    public QueryResult(Query query) {
        this.setStatement(query.getStatement());
        this.setExpectedAggregateRowCount(query.getExpectedAggregateRowCount());
        this.setTenantId(query.getTenantId());
        this.setDdl(query.getDdl());
        this.setQueryGroup(query.getQueryGroup());
        this.setId(query.getId());
    }

    public QueryResult() {
    }

    public Date getStartTime() {
        Date startTime = null;
        for (ThreadTime tt : getThreadTimes()) {
            Date currStartTime = tt.getStartTime();
            if (null != currStartTime) {
                if (null == startTime) {
                    startTime = currStartTime;
                } else if (currStartTime.compareTo(startTime) < 0) {
                    startTime = currStartTime;
                }
            }
        }
        return startTime;
    }

    public int getAvgMaxRunTimeInMs() {
        int totalRunTime = 0;
        for (ThreadTime tt : getThreadTimes()) {
            if (null != tt.getMaxTimeInMs()) {
                totalRunTime += tt.getMaxTimeInMs().getElapsedDurationInMs();
            }
        }
        return totalRunTime / getThreadTimes().size();
    }

    public int getAvgMinRunTimeInMs() {
        int totalRunTime = 0;
        for (ThreadTime tt : getThreadTimes()) {
            if (null != tt.getMinTimeInMs()) {
                totalRunTime += tt.getMinTimeInMs().getElapsedDurationInMs();
            }
        }
        return totalRunTime / getThreadTimes().size();
    }

    public int getAvgRunTimeInMs() {
        int totalRunTime = 0;
        for (ThreadTime tt : getThreadTimes()) {
            if (null != tt.getAvgTimeInMs()) {
                totalRunTime += tt.getAvgTimeInMs();
            }
        }
        return totalRunTime / getThreadTimes().size();
    }

    public List<ResultValue> getCsvRepresentation(ResultUtil util) {
        List<ResultValue> rowValues = new ArrayList<>();
        rowValues.add(new ResultValue(util.convertNull(getStartTimeText())));
        rowValues.add(new ResultValue(util.convertNull(this.getQueryGroup())));
        rowValues.add(new ResultValue(util.convertNull(this.getStatement())));
        rowValues.add(new ResultValue(util.convertNull(this.getTenantId())));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getAvgMaxRunTimeInMs()))));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getAvgRunTimeInMs()))));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getAvgMinRunTimeInMs()))));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getRunCount()))));
        return rowValues;
    }

    private int getRunCount() {
        int totalRunCount = 0;
        for (ThreadTime tt : getThreadTimes()) {
            totalRunCount += tt.getRunCount();
        }
        return totalRunCount;
    }

    public List<List<ResultValue>> getCsvDetailedRepresentation(ResultUtil util, RunMode runMode) {
        List<List<ResultValue>> rows = new ArrayList<>();
        for (ThreadTime tt : getThreadTimes()) {
            for (List<ResultValue> runTime : runMode == RunMode.PERFORMANCE ?
                    tt.getCsvPerformanceRepresentation(util) :
                    tt.getCsvFunctionalRepresentation(util)) {
                List<ResultValue> rowValues = new ArrayList<>();
                rowValues.add(new ResultValue(util.convertNull(getStartTimeText())));
                rowValues.add(new ResultValue(util.convertNull(this.getQueryGroup())));
                rowValues.add(new ResultValue(util.convertNull(this.getStatement())));
                rowValues.add(new ResultValue(util.convertNull(this.getTenantId())));
                rowValues.addAll(runTime);
                rows.add(rowValues);
            }
        }
        return rows;
    }

    private String getStartTimeText() {
        return (null == this.getStartTime())
                ? ""
                : DateUtil.DEFAULT_MS_DATE_FORMATTER.format(this.getStartTime());
    }
}