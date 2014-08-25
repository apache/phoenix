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
package org.apache.phoenix.schema.stat;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple holder class for a single statistics on a column in a region.
 */
public class StatisticsValue {

    protected byte[] name;
    protected byte[] info;
    protected byte[] value;

    public StatisticsValue(byte[] name, byte[] info, byte[] value) {
        this.name = name;
        this.info = info;
        this.value = value;
    }

    public byte[] getType() {
        return name;
    }

    public byte[] getInfo() {
        return info;
    }

    public byte[] getValue() {
        return value;
    }

    protected void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "stat " + Bytes.toString(name) + ": [info:" + Bytes.toString(info) + ", value:" + Bytes.toString(value)
                + "]";
    }
}