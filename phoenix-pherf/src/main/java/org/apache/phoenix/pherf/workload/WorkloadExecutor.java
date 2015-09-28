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

package org.apache.phoenix.pherf.workload;

import org.apache.phoenix.pherf.PherfConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class WorkloadExecutor {
    private static final Logger logger = LoggerFactory.getLogger(WorkloadExecutor.class);
    private final int poolSize;
    private final boolean isPerformance;

    // Jobs can be accessed by multiple threads
    private final Map<Workload, Future> jobs = new ConcurrentHashMap<>();

    private final ExecutorService pool;

    public WorkloadExecutor() throws Exception {
        this(PherfConstants.create().getProperties(PherfConstants.PHERF_PROPERTIES, false));
    }

    public WorkloadExecutor(Properties properties) throws Exception {
        this(properties, new ArrayList(), true);
    }

    public WorkloadExecutor(Properties properties, List<Workload> workloads, boolean isPerformance) throws Exception {
        this.isPerformance = isPerformance;
        this.poolSize =
                (properties.getProperty("pherf.default.threadpool") == null) ?
                        PherfConstants.DEFAULT_THREAD_POOL_SIZE :
                        Integer.parseInt(properties.getProperty("pherf.default.threadpool"));

        this.pool = Executors.newFixedThreadPool(this.poolSize);
        init(workloads);
    }

    public void add(Workload workload) throws Exception {
        this.jobs.put(workload, pool.submit(workload.execute()));
    }

    /**
     * Blocks on waiting for all workloads to finish. If a
     * {@link org.apache.phoenix.pherf.workload.Workload} Requires complete() to be called, it must
     * be called prior to using this method. Otherwise it will block infinitely.
     */
    public void get() {
        for (Workload workload : jobs.keySet()) {
            get(workload);
        }
    }

    /**
     * Calls the {@link java.util.concurrent.Future#get()} method pertaining to this workflow.
     * Once the Future competes, the workflow is removed from the list.
     *
     * @param workload Key entry in the HashMap
     */
    public void get(Workload workload) {
        try {
            Future future = jobs.get(workload);
            future.get();
            jobs.remove(workload);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("", e);
        }
    }

    /**
     * Complete all workloads in the list.
     * Entries in the job Map will persist until {#link WorkloadExecutorNew#get()} is called
     */
    public void complete() {
        for (Workload workload : jobs.keySet()) {
            workload.complete();
        }
    }

    public void shutdown() {
        // Make sure any Workloads still on pool have been properly shutdown
        complete();
        pool.shutdownNow();
    }

    /**
     * TODO This should be removed, Access to the pool should be restriced and callers should Workflows
     *
     * @return {@link ExecutorService} Exposes the underlying thread pool
     */
    public ExecutorService getPool() {
        return pool;
    }

    public boolean isPerformance() {
        return isPerformance;
    }

    private void init(List<Workload> workloads) throws Exception {
        for (Workload workload : workloads) {
            this.jobs.put(workload, pool.submit(workload.execute()));
        }
    }
}