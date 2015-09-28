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
package org.apache.phoenix.monitoring;

import org.apache.phoenix.util.PhoenixStopWatch;

/**
 * 
 * Stop watch that is cognizant of the fact whether or not metrics is enabled.
 * If metrics isn't enabled it doesn't do anything. Otherwise, it delegates
 * calls to a {@code PhoenixStopWatch}.
 *
 */
final class MetricsStopWatch {
    
    private final boolean isMetricsEnabled;
    private final PhoenixStopWatch stopwatch;
    
    MetricsStopWatch(boolean isMetricsEnabled) {
        this.isMetricsEnabled = isMetricsEnabled;
        this.stopwatch = new PhoenixStopWatch();
    }
    
    void start()  {
        if (isMetricsEnabled) {
            stopwatch.start();
        }
    }
    
    void stop() {
        if (isMetricsEnabled) {
            if (stopwatch.isRunning()) {
                stopwatch.stop();
            }
        }
    }
    
    long getElapsedTimeInMs() {
        if (isMetricsEnabled) {
            return stopwatch.elapsedMillis();
        }
        return 0;
    }
}
