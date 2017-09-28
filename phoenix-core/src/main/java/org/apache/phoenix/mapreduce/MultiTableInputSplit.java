/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Holds the target table Scans associated with a source table input split.
 */
public class MultiTableInputSplit extends PhoenixInputSplit {

    private PhoenixInputSplit sourceSplit;
    private List<Scan> targetScans;

    public MultiTableInputSplit() {
    }

    public MultiTableInputSplit(PhoenixInputSplit sourceSplit, List<Scan> targetScans) {
        this.sourceSplit = sourceSplit;
        this.targetScans = targetScans;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sourceSplit.write(out);
        writeScans(out, targetScans);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        targetScans = readScans(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return super.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return super.getLocations();
    }

    List<Scan> getTargetScans() {
        return targetScans;
    }
}
