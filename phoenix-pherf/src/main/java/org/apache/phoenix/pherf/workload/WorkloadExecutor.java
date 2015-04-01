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
import org.apache.phoenix.pherf.PherfConstants.RunMode;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.jmx.MonitorManager;
import org.apache.phoenix.pherf.loaddata.DataLoader;

import org.apache.phoenix.pherf.util.ResourceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WorkloadExecutor {
    private static final Logger logger = LoggerFactory.getLogger(WorkloadExecutor.class);
    private final XMLConfigParser parser;
    private MonitorManager monitor;
    private Future monitorThread;
    private final Properties properties;
    private final int poolSize;

    private final ExecutorService pool;


    public WorkloadExecutor() throws Exception {
        this(new ResourceList().getProperties());
    }

    public WorkloadExecutor(Properties properties) throws Exception{
        this(properties,PherfConstants.DEFAULT_FILE_PATTERN);
    }

    public WorkloadExecutor(Properties properties, String filePattern) throws Exception {
        this(properties,
                new XMLConfigParser(filePattern),
                true);
    }

    public WorkloadExecutor(Properties properties, XMLConfigParser parser, boolean monitor) throws Exception {
        this.parser = parser;
        this.properties = properties;
        this.poolSize = (properties.getProperty("pherf.default.threadpool") == null)
                ? PherfConstants.DEFAULT_THREAD_POOL_SIZE
                : Integer.parseInt(properties.getProperty("pherf.default.threadpool"));

        this.pool = Executors.newFixedThreadPool(this.poolSize);
        if (monitor) {
            initMonitor(Integer.parseInt(properties.getProperty("pherf.default.monitorFrequency")));
        }
    }

    /**
     * Executes all scenarios dataload
     *
     * @throws Exception
     */
    public void executeDataLoad() throws Exception {
        logger.info("\n\nStarting Data Loader...");
        DataLoader dataLoader = new DataLoader(properties, parser);
        dataLoader.execute();
    }

    /**
     * Executes all scenario multi-threaded query sets
     *
     * @param queryHint
     * @throws Exception
     */
    public void executeMultithreadedQueryExecutor(String queryHint, boolean export, RunMode runMode) throws Exception {
        logger.info("\n\nStarting Query Executor...");
        QueryExecutor queryExecutor = new QueryExecutor(parser);
        queryExecutor.execute(queryHint, export, runMode);
    }

    public void shutdown() throws Exception {
		if (null != monitor && monitor.isRunning()) {
            this.monitor.stop();
            this.monitorThread.get(60, TimeUnit.SECONDS);
            this.pool.shutdown();
        }
    }

    // Just used for testing
    public XMLConfigParser getParser() {
        return parser;
    }

    private void initMonitor(int monitorFrequency) throws Exception {
        this.monitor = new MonitorManager(monitorFrequency);
        monitorThread = pool.submit(this.monitor);
    }
}