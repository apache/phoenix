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
package org.apache.phoenix.replication.log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;

public class LogFileHeader implements LogFile.Header {

  /** Magic number for Phoenix Replication Log files */
  static final byte[] MAGIC = Bytes.toBytes("PLOG");
  /** Current major version of the replication log format */
  static final int VERSION_MAJOR = 1;
  /** Current minor version of the replication log format */
  static final int VERSION_MINOR = 0;

  static final int HEADERSIZE = MAGIC.length + 2 * Bytes.SIZEOF_BYTE;

  private int majorVersion = VERSION_MAJOR;
  private int minorVersion = VERSION_MINOR;

  public LogFileHeader() {

  }

  @Override
  public int getMajorVersion() {
    return majorVersion;
  }

  @Override
  public LogFile.Header setMajorVersion(int majorVersion) {
    this.majorVersion = majorVersion;
    return this;
  }

  @Override
  public int getMinorVersion() {
    return minorVersion;
  }

  @Override
  public LogFile.Header setMinorVersion(int minorVersion) {
    this.minorVersion = minorVersion;
    return this;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] magic = new byte[MAGIC.length];
    try {
      in.readFully(magic);
    } catch (EOFException e) {
      throw (IOException) new InvalidLogHeaderException("Short magic").initCause(e);
    }
    if (!Arrays.equals(MAGIC, magic)) {
      throw new InvalidLogHeaderException("Bad magic. Got " + Bytes.toStringBinary(magic)
        + ", expected " + Bytes.toStringBinary(MAGIC));
    }
    try {
      majorVersion = in.readByte();
      minorVersion = in.readByte();
    } catch (EOFException e) {
      throw (IOException) new InvalidLogHeaderException("Short version").initCause(e);
    }
    // Basic version check for now. We assume semver conventions where only higher major
    // versions may be incompatible.
    if (majorVersion > VERSION_MAJOR) {
      throw new InvalidLogHeaderException(
        "Unsupported version. Got major=" + majorVersion + " minor=" + minorVersion
          + ", expected major=" + VERSION_MAJOR + " minor=" + VERSION_MINOR);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(MAGIC);
    out.writeByte(majorVersion);
    out.writeByte(minorVersion);
  }

  @Override
  public int getSerializedLength() {
    return HEADERSIZE;
  }

  @Override
  public String toString() {
    return "LogFileHeader [majorVersion=" + majorVersion + ", minorVersion=" + minorVersion + "]";
  }

}
