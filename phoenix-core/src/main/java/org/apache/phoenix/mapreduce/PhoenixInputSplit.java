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
package org.apache.phoenix.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.query.KeyRange;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Input split class to hold the lower and upper bound range. {@link KeyRange}
 */
public class PhoenixInputSplit extends InputSplit implements Writable {

    private List<Scan> scans;
    private KeyRange keyRange;
   
    /**
     * No Arg constructor
     */
    public PhoenixInputSplit() {
    }
    
   /**
    * 
    * @param keyRange
    */
    public PhoenixInputSplit(final List<Scan> scans) {
        Preconditions.checkNotNull(scans);
        Preconditions.checkState(!scans.isEmpty());
        this.scans = scans;
        init();
    }
    
    public List<Scan> getScans() {
        return scans;
    }
    
    public KeyRange getKeyRange() {
        return keyRange;
    }
    
    private void init() {
        this.keyRange = KeyRange.getKeyRange(scans.get(0).getStartRow(), scans.get(scans.size()-1).getStopRow());
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        int count = WritableUtils.readVInt(input);
        scans = Lists.newArrayListWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            byte[] protoScanBytes = new byte[WritableUtils.readVInt(input)];
            input.readFully(protoScanBytes);
            ClientProtos.Scan protoScan = ClientProtos.Scan.parseFrom(protoScanBytes);
            Scan scan = ProtobufUtil.toScan(protoScan);
            scans.add(scan);
        }
        init();
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        Preconditions.checkNotNull(scans);
        WritableUtils.writeVInt(output, scans.size());
        for (Scan scan : scans) {
            ClientProtos.Scan protoScan = ProtobufUtil.toScan(scan);
            byte[] protoScanBytes = protoScan.toByteArray();
            WritableUtils.writeVInt(output, protoScanBytes.length);
            output.write(protoScanBytes);
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
         return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + keyRange.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (!(obj instanceof PhoenixInputSplit)) { return false; }
        PhoenixInputSplit other = (PhoenixInputSplit)obj;
        if (keyRange == null) {
            if (other.keyRange != null) { return false; }
        } else if (!keyRange.equals(other.keyRange)) { return false; }
        return true;
    }

}
