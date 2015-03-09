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
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;

public class ThreadTime {
    private List<RunTime> runTimesInMs = Collections.synchronizedList(new ArrayList<RunTime>());
    private String threadName;

    public synchronized List<RunTime> getRunTimesInMs() {
        return this.runTimesInMs;
    }

    public synchronized void setRunTimesInMs(List<RunTime> runTimesInMs) {
        this.runTimesInMs = runTimesInMs;
    }

    /**
     * @return The earliest start time out of collected run times.
     */
    public Date getStartTime() {
        if (getRunTimesInMs().isEmpty()) return new Date(0);

        Date startTime = null;
        synchronized (getRunTimesInMs()) {
            for (RunTime runTime : getRunTimesInMs()) {
                if (null != runTime.getStartTime()) {
                    Date currStartTime = new Date(runTime.getStartTime().getTime());
                    if (null == startTime) {
                        startTime = currStartTime;
                    } else if (currStartTime.compareTo(startTime) < 0) {
                        startTime = currStartTime;
                    }
                } else {
                    startTime = new Date(0);
                }
            }
        }
        return startTime;
    }

    public RunTime getMinTimeInMs() {
        if (getRunTimesInMs().isEmpty()) return null;
        return Collections.min(getRunTimesInMs());
    }

    public Integer getAvgTimeInMs() {
        if (getRunTimesInMs().isEmpty()) return null;

        Integer totalTimeInMs = new Integer(0);
        for (RunTime runTime : getRunTimesInMs()) {
            if (null != runTime.getElapsedDurationInMs()) {
                totalTimeInMs += runTime.getElapsedDurationInMs();
            }
        }
        return totalTimeInMs / getRunTimesInMs().size();
    }

    public RunTime getMaxTimeInMs() {
        if (getRunTimesInMs().isEmpty()) return null;
        return Collections.max(getRunTimesInMs());
    }

    @XmlAttribute()
    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }
    
    private String parseThreadName(boolean getConcurrency) {
    	if (getThreadName() == null || !getThreadName().contains(",")) return null;
    	String[] threadNameSet = getThreadName().split(",");
    	if (getConcurrency) {
    		return threadNameSet[1];}
    	else {
    		return threadNameSet[0];
    	}
    }

    public List<List<ResultValue>> getCsvPerformanceRepresentation(ResultUtil util) {
        List<List<ResultValue>> rows = new ArrayList<>();

        for (int i = 0; i < getRunTimesInMs().size(); i++) {
            List<ResultValue> rowValues = new ArrayList(getRunTimesInMs().size());
            rowValues.add(new ResultValue(util.convertNull(parseThreadName(false))));
            rowValues.add(new ResultValue(util.convertNull(parseThreadName(true))));
            rowValues.add(new ResultValue(String.valueOf(getRunTimesInMs().get(i).getResultRowCount())));
            if (getRunTimesInMs().get(i).getMessage() == null) {
                rowValues.add(new ResultValue(util.convertNull(String.valueOf(getRunTimesInMs().get(i).getElapsedDurationInMs()))));
            } else {
                rowValues.add(new ResultValue(util.convertNull(getRunTimesInMs().get(i).getMessage())));
            }
            rows.add(rowValues);
        }
        return rows;
    }

    public List<List<ResultValue>> getCsvFunctionalRepresentation(ResultUtil util) {
        List<List<ResultValue>> rows = new ArrayList<>();

        for (int i = 0; i < getRunTimesInMs().size(); i++) {
            List<ResultValue> rowValues = new ArrayList<>(getRunTimesInMs().size());
            rowValues.add(new ResultValue(util.convertNull(parseThreadName(false))));
            rowValues.add(new ResultValue(util.convertNull(parseThreadName(true))));
            rowValues.add(new ResultValue(util.convertNull(getRunTimesInMs().get(i).getMessage())));
            rowValues.add(new ResultValue(util.convertNull(getRunTimesInMs().get(i).getExplainPlan())));
            rows.add(rowValues);
        }
        return rows;
    }

    public int getRunCount() {
        if (getRunTimesInMs().isEmpty()) return 0;
        return getRunTimesInMs().size();
    }
}
