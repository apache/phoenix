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

import javax.xml.bind.annotation.XmlAttribute;
import java.util.Comparator;
import java.util.Date;

public class RunTime implements Comparator<RunTime>, Comparable<RunTime> {
    private Date startTime;
    private Integer elapsedDurationInMs;
    private String message;
    private Long resultRowCount;
    private String explainPlan;

    @SuppressWarnings("unused") public RunTime() {
    }

    @SuppressWarnings("unused") public RunTime(Integer elapsedDurationInMs) {
        this(null, elapsedDurationInMs);
    }

    public RunTime(Long resultRowCount, Integer elapsedDurationInMs) {
        this(null, resultRowCount, elapsedDurationInMs);
    }

    public RunTime(Date startTime, Long resultRowCount, Integer elapsedDurationInMs) {
        this(null, null, startTime, resultRowCount, elapsedDurationInMs);
    }

    public RunTime(String message, Date startTime, Long resultRowCount,
            Integer elapsedDurationInMs) {
        this(message, null, startTime, resultRowCount, elapsedDurationInMs);
    }

    public RunTime(String message, String explainPlan, Date startTime, Long resultRowCount,
            Integer elapsedDurationInMs) {
        this.elapsedDurationInMs = elapsedDurationInMs;
        this.startTime = startTime;
        this.resultRowCount = resultRowCount;
        this.message = message;
        this.explainPlan = explainPlan;
    }

    @XmlAttribute() public Date getStartTime() {
        return startTime;
    }

    @SuppressWarnings("unused") public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    @XmlAttribute() public Integer getElapsedDurationInMs() {
        return elapsedDurationInMs;
    }

    @SuppressWarnings("unused") public void setElapsedDurationInMs(Integer elapsedDurationInMs) {
        this.elapsedDurationInMs = elapsedDurationInMs;
    }

    @Override public int compare(RunTime r1, RunTime r2) {
        return r1.getElapsedDurationInMs().compareTo(r2.getElapsedDurationInMs());
    }

    @Override public int compareTo(RunTime o) {
        return compare(this, o);
    }

    @XmlAttribute() public String getMessage() {
        return message;
    }

    @SuppressWarnings("unused") public void setMessage(String message) {
        this.message = message;
    }

    @XmlAttribute() public String getExplainPlan() {
        return explainPlan;
    }

    @SuppressWarnings("unused") public void setExplainPlan(String explainPlan) {
        this.explainPlan = explainPlan;
    }

    @XmlAttribute() public Long getResultRowCount() {
        return resultRowCount;
    }

    @SuppressWarnings("unused") public void setResultRowCount(Long resultRowCount) {
        this.resultRowCount = resultRowCount;
    }
}