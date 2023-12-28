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
package org.apache.phoenix.optimize;

import java.util.Objects;

/**
 * Optimizer cost in terms of CPU, memory, and I/O usage, the unit of which is now the
 * number of bytes processed.
 *
 */
public class Cost implements Comparable<Cost> {
    /** The unknown cost. */
    public static final Cost UNKNOWN = new Cost(Double.NaN, Double.NaN, Double.NaN) {
        @Override
        public String toString() {
            return "{unknown}";
        }
    };

    /** The zero cost. */
    public static final Cost ZERO = new Cost(0, 0, 0) {
        @Override
        public String toString() {
            return "{zero}";
        }        
    };

    private final double cpu;
    private final double memory;
    private final double io;

    public Cost(double cpu, double memory, double io) {
        this.cpu = cpu;
        this.memory = memory;
        this.io = io;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMemory() {
        return memory;
    }

    public double getIo() {
        return io;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    public Cost plus(Cost other) {
        if (isUnknown() || other.isUnknown()) {
            return UNKNOWN;
        }

        return new Cost(
                this.cpu + other.cpu,
                this.memory + other.memory,
                this.io + other.io);
    }

    public Cost multiplyBy(double factor) {
        if (isUnknown()) {
            return UNKNOWN;
        }

        return new Cost(
                this.cpu * factor,
                this.memory * factor,
                this.io * factor);
    }

    // TODO right now for simplicity, we choose to ignore CPU and memory costs. We may
    // add those into account as our cost model mature.
    @Override
    public int compareTo(Cost other) {
        if (isUnknown() && other.isUnknown()) {
            return 0;
        } else if (isUnknown() && !other.isUnknown()) {
            return 1;
        } else if (!isUnknown() && other.isUnknown()) {
            return -1;
        }

        double d = this.io - other.io;
        return d == 0 ? 0 : (d > 0 ? 1 : -1);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
                || (obj instanceof Cost && this.compareTo((Cost) obj) == 0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpu, memory, io);
    }

    @Override
    public String toString() {
        return "{cpu: " + cpu + ", memory: " + memory + ", io: " + io + "}";
    }
}
