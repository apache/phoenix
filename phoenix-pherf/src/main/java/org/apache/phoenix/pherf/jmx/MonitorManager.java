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

package org.apache.phoenix.pherf.jmx;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.exception.FileLoaderRuntimeException;
import org.apache.phoenix.pherf.jmx.monitors.Monitor;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.CSVResultHandler;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultHandler;
import org.apache.phoenix.util.DateUtil;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class starts JMX stats for the configured monitors. Monitors should be configured in MonitorDetails Enum.
 * Each stat implements {@link org.apache.phoenix.pherf.jmx.monitors.Monitor}.
 *
 * For the duration of any Pherf run, when the configured {@link org.apache.phoenix.pherf.PherfConstants#MONITOR_FREQUENCY}
 * is reached a snapshot of each monitor is taken and dumped out to a log file.
 */
public class MonitorManager implements Runnable {
    // List of MonitorDetails for all the running monitors.
    // TODO Move this out to config. Possible use Guice and use IOC to inject it in.
    private static final List<MonitorDetails> MONITOR_DETAILS_LIST =
            Arrays.asList(MonitorDetails.values());
    private final ResultHandler resultHandler;
    private final long monitorFrequency;
    private AtomicLong rowCount;
    private volatile boolean shouldStop = false;
    private volatile boolean isRunning = false;

    public MonitorManager() throws Exception {
        this(PherfConstants.MONITOR_FREQUENCY);
    }

    /**
     *
     * @param monitorFrequency Frequency at which monitor stats are written to a log file.
     * @throws Exception
     */
    public MonitorManager(long monitorFrequency) throws Exception {
        this.monitorFrequency = monitorFrequency;
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        // Register all the monitors to JMX
        for (MonitorDetails monitorDetails : MONITOR_DETAILS_LIST) {
            StandardMBean bean = new StandardMBean(monitorDetails.getMonitor(), Monitor.class);
            ObjectName monitorThreadStatName = new ObjectName(monitorDetails.toString());
            try {
                mbs.registerMBean(bean, monitorThreadStatName);
            } catch (InstanceAlreadyExistsException e) {
                mbs.unregisterMBean(monitorThreadStatName);
                mbs.registerMBean(bean, monitorThreadStatName);
            }
        }
        rowCount = new AtomicLong(0);
        this.resultHandler = new CSVResultHandler(PherfConstants.MONITOR_FILE_NAME, ResultFileDetails.CSV);
    }

    @Override
    public void run() {
        try {
            while (!shouldStop()) {
                isRunning = true;
                List rowValues = new ArrayList<String>();
                synchronized (resultHandler) {
                    for (MonitorDetails monitorDetails : MONITOR_DETAILS_LIST) {
                        rowValues.clear();
                        try {
                            StandardMBean bean = new StandardMBean(monitorDetails.getMonitor(), Monitor.class);

                            Calendar calendar = new GregorianCalendar();
                            rowValues.add(monitorDetails);

                            rowValues.add(((Monitor) bean.getImplementation()).getStat());
                            rowValues.add(DateUtil.DEFAULT_MS_DATE_FORMATTER.format(calendar.getTime()));
                            Result
                                    result = new Result(ResultFileDetails.CSV, ResultFileDetails.CSV_MONITOR.getHeader().toString(), rowValues);
                            resultHandler.write(result);
                        } catch (Exception e) {
                            throw new FileLoaderRuntimeException("Could not log monitor result.", e);
                        }
                        rowCount.getAndIncrement();
                    }
                    try {
                        Thread.sleep(getMonitorFrequency());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            try {
                isRunning = false;
                if (resultHandler != null) {
                    resultHandler.flush();
                    resultHandler.close();

                }
            } catch (Exception e) {
                throw new FileLoaderRuntimeException("Could not close monitor results.", e);
            }
        }

    }

    public long getMonitorFrequency() {
        return monitorFrequency;
    }

    public synchronized boolean shouldStop() {
        return shouldStop;
    }

    public synchronized void stop() {
        this.shouldStop = true;
    }

    public synchronized long getRowCount() {
        return rowCount.get();
    }

    public synchronized boolean isRunning() {
        return isRunning;
    }

    /**
     * This method should really only be used for testing
     *
     * @return List < {@link org.apache.phoenix.pherf.result.Result} >
     * @throws IOException
     */
    public synchronized List<Result> readResults() throws Exception {
        ResultHandler handler = null;
        try {
            if (resultHandler.isClosed()) {
                handler = new CSVResultHandler(PherfConstants.MONITOR_FILE_NAME, ResultFileDetails.CSV);
                return handler.read();
            } else {
                return resultHandler.read();
            }
        } catch (Exception e) {
            throw new FileLoaderRuntimeException("Could not close monitor results.", e);
        } finally {
            if (handler != null) {
                handler.close();
            }
        }
    }
}
