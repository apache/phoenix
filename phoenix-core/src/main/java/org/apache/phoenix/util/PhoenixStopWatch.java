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
package org.apache.phoenix.util;

/**
 * Bare-bones implementation of a stop watch that only measures time in milliseconds. If you want to be fancy then
 * please use the guava Stopwatch. However, be warned that the Guava's Stopwatch is a beta class and is subject to
 * incompatible changes and removal. So save the future upgrade pain and use this class instead.
 */
public class PhoenixStopWatch {
    private boolean isRunning;
    private long startTime;
    private long elapsedTimeMs;

    /**
     * Creates a new stop watch without starting it.
     */
    public PhoenixStopWatch() {}

    /**
     * Starts the stopwatch.
     * 
     * @return this {@code PhoenixStopWatch} instance
     * @throws IllegalStateException
     *             if the stopwatch is already running.
     */
    public PhoenixStopWatch start() {
        long currentTime = System.currentTimeMillis();
        if (isRunning) { throw new IllegalStateException("Watch is already running"); }
        startTime = currentTime;
        isRunning = true;
        return this;
    }

    /**
     * Stops the stopwatch. Future calls to {@link #elapsedMillis()} will return the fixed duration that had elapsed up
     * to this point.
     * 
     * @return this {@code PhoenixStopWatch} instance
     * @throws IllegalStateException
     *             if the stopwatch is already stopped.
     */
    public PhoenixStopWatch stop() {
        long currentTime = System.currentTimeMillis();
        if (!isRunning) { throw new IllegalStateException("Watch wasn't started"); }
        elapsedTimeMs = currentTime - startTime;
        startTime = 0;
        isRunning = false;
        return this;
    }

    /**
     * Returns the current elapsed time shown on this stopwatch, expressed in milliseconds.
     */
    public long elapsedMillis() {
        return elapsedTimeMs;
    }

    /**
     * Returns {@code true} if {@link #start()} has been called on this stopwatch, and {@link #stop()} has not been
     * called since the last call to {@code start()}.
     */
    public boolean isRunning() {
        return isRunning;
    }
}
