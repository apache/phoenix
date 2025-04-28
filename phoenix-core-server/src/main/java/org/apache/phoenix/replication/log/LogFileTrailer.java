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

public class LogFileTrailer implements LogFile.Trailer {
    private int majorVersion = LogFileHeader.VERSION_MAJOR;
    private int minorVersion = LogFileHeader.VERSION_MINOR;
    private long recordCount;
    private long blockCount;
    private long blocksStartOffset;
    private long trailerStartOffset;

    public static final int VERSION_AND_MAGIC_SIZE = LogFileHeader.MAGIC.length
        + 2 * Bytes.SIZEOF_BYTE;
    public static final int FIXED_TRAILER_SIZE = Bytes.SIZEOF_LONG * 4 + VERSION_AND_MAGIC_SIZE;

    public LogFileTrailer() {

    }

    // Add methods to set/get Protobuf metadata when needed in the future, e.g.:
    // public void setMetadata(LogTrailerMetadata metadata) { ... }
    // public LogTrailerMetadata getMetadata() throws InvalidProtocolBufferException { ... }

    @Override
    public int getMajorVersion() {
        return majorVersion;
    }

    @Override
    public int getMinorVersion() {
        return minorVersion;
    }

    @Override
    public LogFile.Trailer setMajorVersion(int majorVersion) {
        this.majorVersion = majorVersion;
        return this;
    }

    @Override
    public LogFile.Trailer setMinorVersion(int minorVersion) {
        this.minorVersion = minorVersion;
        return this;
    }

    @Override
    public long getRecordCount() {
        return recordCount;
    }

    @Override
    public LogFile.Trailer setRecordCount(long recordCount) {
        this.recordCount = recordCount;
        return this;
    }

    @Override
    public long getBlockCount() {
        return blockCount;
    }

    @Override
    public LogFile.Trailer setBlockCount(long blockCount) {
        this.blockCount = blockCount;
        return this;
    }

    @Override
    public long getBlocksStartOffset() {
        return blocksStartOffset;
    }

    @Override
    public LogFile.Trailer setBlocksStartOffset(long offset) {
        this.blocksStartOffset = offset;
        return this;
    }

    @Override
    public long getTrailerStartOffset() {
        return trailerStartOffset;
    }

    @Override
    public LogFile.Trailer setTrailerStartOffset(long offset) {
        this.trailerStartOffset = offset;
        return this;
    }

    public void readFixedFields(DataInput in) throws IOException {
        this.recordCount = in.readLong();
        this.blockCount = in.readLong();
        this.blocksStartOffset = in.readLong();
        this.trailerStartOffset = in.readLong();
        this.majorVersion = in.readByte();
        this.minorVersion = in.readByte();
        // Basic version check for now
        if (this.majorVersion != LogFileHeader.VERSION_MAJOR
                && this.minorVersion > LogFileHeader.VERSION_MINOR) {
            throw new IOException("Unsupported LogFile version. Got major=" + majorVersion
                + " minor=" + minorVersion + ", expected major=" + LogFileHeader.VERSION_MAJOR
                + " minor=" + LogFileHeader.VERSION_MINOR);
        }
        byte[] magic = new byte[LogFileHeader.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(LogFileHeader.MAGIC, magic)) {
            throw new IOException("Invalid LogFile magic. Got " + Bytes.toStringBinary(magic)
                + ", expected " + Bytes.toStringBinary(LogFileHeader.MAGIC));
        }
    }

    public void readMetadata(DataInput in) throws IOException {
        int protoSize = in.readInt();
        if (protoSize < 0) {
            throw new IOException("Invalid Protobuf message size in LogFile trailer");
        }
        if (protoSize > 0) {
            byte[] protoBytes = new byte[protoSize];
            in.readFully(protoBytes);
            // Deserialize protobuf when we have something to read
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        readFixedFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(0); // Protobuf bytes
        // Serialize protobuf when we have a message to write
        out.writeLong(this.recordCount);
        out.writeLong(this.blockCount);
        out.writeLong(this.blocksStartOffset);
        out.writeLong(this.trailerStartOffset);
        out.writeByte(this.getMajorVersion());
        out.writeByte(this.getMinorVersion());
        out.write(LogFileHeader.MAGIC);
    }

    @Override
    public int getSerializedLength() {
        return Bytes.SIZEOF_INT // Protobuf message length
            // Add the size of the Protobuf serialized message when we have one
            + FIXED_TRAILER_SIZE;
    }

    public static boolean isValidTrailer(final FileSystem fs, final Path path) throws IOException {
        try (FSDataInputStream in = fs.open(path)) {
            return isValidTrailer(in, fs.getFileStatus(path).getLen());
        }
    }

    public static boolean isValidTrailer(FSDataInputStream in, long length) throws IOException {
        long offset = length - VERSION_AND_MAGIC_SIZE;
        if (offset < 0) {
            return false;
        }
        in.seek(offset);
        byte[] magic = new byte[LogFileHeader.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(LogFileHeader.MAGIC, magic)) {
            return false;
        }
        int majorVersion = in.readByte();
        int minorVersion = in.readByte();
        // Basic version check for now
        if (majorVersion != LogFileHeader.VERSION_MAJOR
                && minorVersion > LogFileHeader.VERSION_MINOR) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "LogFileTrailer [majorVersion=" + majorVersion + ", minorVersion=" + minorVersion
            + ", recordCount=" + recordCount + ", blockCount=" + blockCount
            + ", blocksStartOffset=" + blocksStartOffset + ", trailerStartOffset="
            + trailerStartOffset + "]";
    }

}
