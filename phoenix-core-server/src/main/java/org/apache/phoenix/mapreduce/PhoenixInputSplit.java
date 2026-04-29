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

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Input split class to hold the lower and upper bound range. {@link KeyRange}
 */
public class PhoenixInputSplit extends InputSplit implements Writable {

  private List<Scan> scans;
  private List<KeyRange> keyRanges;
  private String regionLocation = null;
  private long splitSize = 0;

  /**
   * No Arg constructor
   */
  public PhoenixInputSplit() {
  }

  /**
   *
   */
  public PhoenixInputSplit(final List<Scan> scans) {
    this(scans, 0, null);
  }

  public PhoenixInputSplit(final List<Scan> scans, long splitSize, String regionLocation) {
    Preconditions.checkNotNull(scans);
    Preconditions.checkState(!scans.isEmpty());
    this.scans = scans;
    this.splitSize = splitSize;
    this.regionLocation = regionLocation;
    init();
  }

  public List<Scan> getScans() {
    return scans;
  }

  /**
   * Returns the overall KeyRange spanning this split. For coalesced splits, spans from the first
   * region's lower bound to the last region's upper bound. Computed on-demand from keyRanges.
   * @return KeyRange spanning the entire split, or null if keyRanges is empty
   */
  public KeyRange getKeyRange() {
    if (keyRanges == null || keyRanges.isEmpty()) {
      return null;
    }
    return KeyRange.getKeyRange(keyRanges.get(0).getLowerRange(),
      keyRanges.get(keyRanges.size() - 1).getUpperRange());
  }

  /**
   * Returns all KeyRanges for this split. For coalesced splits, returns multiple KeyRanges (one per
   * region). For non-coalesced splits, returns a single-element list.
   * @return List of KeyRanges, never null
   */
  public List<KeyRange> getKeyRanges() {
    return keyRanges;
  }

  /**
   * Checks if this split is coalesced (contains multiple regions).
   * @return true if split contains multiple regions
   */
  public boolean isCoalesced() {
    return keyRanges.size() > 1;
  }

  private void init() {
    // Initialize keyRanges from scans
    this.keyRanges = Lists.newArrayListWithExpectedSize(scans.size());
    for (Scan scan : scans) {
      KeyRange kr = KeyRange.getKeyRange(scan.getStartRow(), scan.getStopRow());
      this.keyRanges.add(kr);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    regionLocation = WritableUtils.readString(input);
    splitSize = WritableUtils.readVLong(input);
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
    WritableUtils.writeString(output, regionLocation);
    WritableUtils.writeVLong(output, splitSize);

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
    return splitSize;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    if (regionLocation == null) {
      return new String[] {};
    } else {
      return new String[] { regionLocation };
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    KeyRange range = getKeyRange();
    result = prime * result + (range == null ? 0 : range.hashCode());
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
    KeyRange thisRange = getKeyRange();
    KeyRange otherRange = other.getKeyRange();
    if (thisRange == null) {
      if (otherRange != null) {
        return false;
      }
    } else if (!thisRange.equals(otherRange)) {
      return false;
    }
    return true;
  }

  public void setLength(long length) {
    this.splitSize = length;
  }

}
