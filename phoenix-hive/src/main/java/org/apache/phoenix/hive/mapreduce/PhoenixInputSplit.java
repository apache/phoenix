/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.phoenix.query.KeyRange;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * InputSplit implementation. Represents the data to be processed by an individual Mapper
 */
public class PhoenixInputSplit extends FileSplit implements InputSplit {

    private List<Scan> scans;
    private KeyRange keyRange;

    private long regionSize;

    //  query is in the  split because it is not delivered in jobConf.
    private String query;

    public PhoenixInputSplit() {
    }

    public PhoenixInputSplit(final List<Scan> scans, Path dummyPath, String regionLocation, long
            length) {
        super(dummyPath, 0, 0, new String[]{regionLocation});

        regionSize = length;

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

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    private void init() {
        this.keyRange = KeyRange.getKeyRange(scans.get(0).getStartRow(), scans.get(scans.size() -
                1).getStopRow());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Preconditions.checkNotNull(scans);
        WritableUtils.writeVInt(out, scans.size());
        for (Scan scan : scans) {
            ClientProtos.Scan protoScan = ProtobufUtil.toScan(scan);
            byte[] protoScanBytes = protoScan.toByteArray();
            WritableUtils.writeVInt(out, protoScanBytes.length);
            out.write(protoScanBytes);
        }

        WritableUtils.writeString(out, query);
        WritableUtils.writeVLong(out, regionSize);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int count = WritableUtils.readVInt(in);
        scans = Lists.newArrayListWithExpectedSize(count);
        for (int i = 0; i < count; i++) {
            byte[] protoScanBytes = new byte[WritableUtils.readVInt(in)];
            in.readFully(protoScanBytes);
            ClientProtos.Scan protoScan = ClientProtos.Scan.parseFrom(protoScanBytes);
            Scan scan = ProtobufUtil.toScan(protoScan);
            scans.add(scan);
        }
        init();

        query = WritableUtils.readString(in);
        regionSize = WritableUtils.readVLong(in);
    }

    @Override
    public long getLength() {
        return regionSize;
    }

    @Override
    public String[] getLocations() throws IOException {
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
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PhoenixInputSplit)) {
            return false;
        }
        PhoenixInputSplit other = (PhoenixInputSplit) obj;
        if (keyRange == null) {
            if (other.keyRange != null) {
                return false;
            }
        } else if (!keyRange.equals(other.keyRange)) {
            return false;
        }
        return true;
    }
}
