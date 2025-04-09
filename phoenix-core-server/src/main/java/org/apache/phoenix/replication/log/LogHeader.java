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
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

public class LogHeader implements Log.Header {
    private int majorVersion = Log.VERSION_MAJOR;
    private int minorVersion = Log.VERSION_MINOR;
    private int blockChecksumVersion = Log.CHECKSUM_VERSION;

    public static int HEADER_SIZE = Log.MAGIC.length + 3 * Bytes.SIZEOF_BYTE;

    public LogHeader() {
    }

    @Override
    public int getMajorVersion() {
        return majorVersion;
    }

    @Override
    public Log.Header setMajorVersion(int majorVersion) {
        this.majorVersion = majorVersion;
        return this;
    }

    @Override
    public int getMinorVersion() {
        return minorVersion;
    }

    @Override
    public Log.Header setMinorVersion(int minorVersion) {
        this.minorVersion = minorVersion;
        return this;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] magic = new byte[Log.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(Log.MAGIC, magic)) {
            throw new IOException("Invalid Replication Log file magic. Got "
                + Bytes.toStringBinary(magic) + ", expected "
                + Bytes.toStringBinary(Log.MAGIC));
        }
        majorVersion = in.readByte();
        minorVersion = in.readByte();
        // Basic version check for now
        if (majorVersion != Log.VERSION_MAJOR && minorVersion > Log.VERSION_MINOR) {
            throw new IOException("Unsupported Log version. Got major=" + majorVersion
                + " minor=" + minorVersion + ", expected major=" + Log.VERSION_MAJOR
                + " minor=" + Log.VERSION_MINOR);
        }
        blockChecksumVersion = in.readByte();
        // Basic version check for now
        if (blockChecksumVersion != Log.CHECKSUM_VERSION) {
            throw new IOException("Unsupported Log checksum version. Got "
                + blockChecksumVersion + ", expected " + Log.CHECKSUM_VERSION);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(Log.MAGIC);
        out.writeByte(majorVersion);
        out.writeByte(minorVersion);
        out.writeByte(blockChecksumVersion);
    }

    @Override
    public int getSerializedLength() {
        return HEADER_SIZE;
    }

    public static boolean isValidHeader(final FileSystem fs, final Path path) throws IOException {
        if (fs.getFileStatus(path).getLen() < HEADER_SIZE) {
            return false;
        }
        try (FSDataInputStream in = fs.open(path)) {
            return isValidHeader(in);
        }
    }

    public static boolean isValidHeader(FSDataInputStream in) throws IOException {
        in.seek(0);
        byte[] magic = new byte[Log.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(Log.MAGIC, magic)) {
            return false;
        }
        int majorVersion = in.readByte();
        int minorVersion = in.readByte();
        // Basic version check for now
        if (majorVersion != Log.VERSION_MAJOR && minorVersion > Log.VERSION_MINOR) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "LogHeader [majorVersion=" + majorVersion + ", minorVersion=" + minorVersion
            + ", blockChecksumVersion=" + blockChecksumVersion + "]";
    }

}
