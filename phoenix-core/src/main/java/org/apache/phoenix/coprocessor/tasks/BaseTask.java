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

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.schema.task.Task;

public abstract class BaseTask {
    protected long timeMaxInterval;
    protected RegionCoprocessorEnvironment env;
    public void init(RegionCoprocessorEnvironment env, Long interval) {
        this.env = env;
        this.timeMaxInterval = interval;
    }
    public abstract TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord);

    public abstract TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord) throws Exception;
}
