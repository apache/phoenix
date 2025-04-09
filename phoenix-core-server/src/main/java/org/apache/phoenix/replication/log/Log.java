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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Defines the structure and constants for Phoenix Replication Log files.
 * Provides interfaces for reading and writing these logs.
 */
public final class Log {

    /** Magic number for Phoenix Replication Log files */
    public static final byte[] MAGIC = Bytes.toBytes("PLOG");

    /** Current major version of the replication log format */
    public static final byte VERSION_MAJOR = 1;
    /** Current minor version of the replication log format */
    public static final byte VERSION_MINOR = 0;

    /** Current version of the replication log checksum */
    public static final byte CHECKSUM_VERSION = 1;
    /** Size of the block checksum trailer (CRC64) */
    public static final int CHECKSUM_SIZE = Bytes.SIZEOF_LONG;

    /** Represents the file header */
    public interface Header {
        int getMajorVersion();
        Header setMajorVersion(int majorVersion);
        int getMinorVersion();
        Header setMinorVersion(int minorVersion);
        int getSerializedLength();
        void readFields(DataInput in) throws IOException;
        void write(DataOutput out) throws IOException;
    }

    /** Represents the header of a single block within the log file */
    public interface BlockHeader {
        /** Magic number for Phoenix Replication Log blocks */
        public static final byte[] MAGIC = Bytes.toBytes("PBLK");
        /** Current version of the replication log block header */
        public static final byte VERSION = 1;
        int getVersion();
        Compression.Algorithm getCompression();
        BlockHeader setCompression(Compression.Algorithm compression);
        int getUncompressedSize();
        BlockHeader setUncompressedSize(int uncompressedSize);
        int getCompressedSize();
        BlockHeader setCompressedSize(int compressedSize);
        int getSerializedLength();
        void readFields(DataInput in) throws IOException;
        void write(DataOutput out) throws IOException;
    }

    /** Represents the file trailer */
    public interface Trailer {
        int getMajorVersion();
        Trailer setMajorVersion(int majorVersion);
        int getMinorVersion();
        Trailer setMinorVersion(int minorVersion);
        long getRecordCount();
        Trailer setRecordCount(long recordCount);
        long getBlockCount();
        Trailer setBlockCount(long blockCount);
        long getBlocksStartOffset();
        Trailer setBlocksStartOffset(long offset);
        long getTrailerStartOffset();
        Trailer setTrailerStartOffset(long offset);
        int getSerializedLength();
        void readFields(DataInput in) throws IOException;
        void write(DataOutput out) throws IOException;
    }

    /** Represents a single logical change */
    public interface Record {
        MutationType getMutationType();
        Record setMutationType(MutationType mutationType);
        String getSchemaObjectName();
        Record setSchemaObjectName(String schemaObjectName);
        long getCommitId();
        Record setCommitId(long commitId);
        byte[] getRowKey();
        Record setRowKey(byte[] rowKey);
        long getTimestamp();
        Record setTimestamp(long timestamp);
        int getColumnCount();
        Iterable<Map.Entry<byte[], byte[]>> getColumnValues();
        Record addColumnValue(byte[] columnName, byte[] value);
        Record clearColumnValues();
        int getSerializedLength();
        Record setSerializedLength(int serializedLength);
    }

    /**
     * Defines the types of mutations that can be stored in a Record.
     * Mirrors relevant parts of HBase Cell.Type.
     */
    public enum MutationType {
        PUT((byte)4),
        DELETE((byte)8),                 // Delete for entire row at specified ts
        DELETE_COLUMN((byte)12),         // Delete for specific column at specified ts
        DELETE_FAMILY((byte)14),         // Delete for entire column family at specified ts
        DELETE_FAMILY_VERSION((byte)10); // Delete for specific column family version

        private final byte code;

        MutationType(byte code) {
            this.code = code;
        }

        public byte getCode() {
            return code;
        }

        public static MutationType codeToType(byte code) {
            for (MutationType t : values()) {
                if (t.code == code) {
                    return t;
                }
            }
            throw new IllegalArgumentException("Unknown MutationType code: " + code);
        }

        public static MutationType fromHBaseCellType(Cell.Type type) {
             switch (type) {
                case Put:
                    return PUT;
                case Delete:
                    return DELETE;
                case DeleteColumn:
                    return DELETE_COLUMN;
                case DeleteFamily:
                    return DELETE_FAMILY;
                case DeleteFamilyVersion:
                    return DELETE_FAMILY_VERSION;
                default:
                    throw new IllegalArgumentException("Unsupported MutationType: " + type);
            }
        }
    }

    /** Interface for writing replication logs */
    public interface Writer extends Closeable {
        void init(LogWriterContext context) throws IOException;
        void append(Record record) throws IOException;
        /** Flushes buffered data and syncs to the underlying filesystem */
        void sync() throws IOException;
        /** Returns the current length of the log file */
        long getLength() throws IOException;
    }

    /** Interface for reading replication logs */
    public interface Reader extends Closeable, Iterable<Record> {
        void init(LogReaderContext context) throws IOException;
        /** Returns the next record, or null if the end of the log is reached */
        Record next() throws IOException;
        /** Returns the next record, potentially reusing the provided record object */
        Record next(Record reuse) throws IOException;
        /** Returns the file header */
        Header getHeader();
        /** Returns the file trailer, if present and valid */
        Trailer getTrailer();
    }

    /** Interface for encoding/decoding Records within a block's buffer */
    public interface Codec {
        Encoder getEncoder(DataOutput out);
        Decoder getDecoder(DataInput in);
        Decoder getDecoder(ByteBuffer buffer);

        /** Interface for encoding Records to a DataOutput stream */
        interface Encoder {
             void write(Record record) throws IOException;
             // NOTE: The encoder must set serializedLength in the Record after writing.
        }

        /** Interface for decoding Records from a DataInput stream or ByteBuffer */
        interface Decoder {
            /**
             * Moves the decoder to the next record in the buffer. If a non-null {@code reuse}
             * object is provided and is compatible, it will be populated with the data instead
             * of creating a new object internally.
             * @param reuse An optional existing Record object to reuse.
             * @return true if a record was successfully read, false if the end of the stream
             *         is reached.
             * @throws IOException if an error occurs during reading.
             */
            boolean advance(Record reuse) throws IOException;
            /**
             * Returns the current record, which might be the object passed in the last successful
             * call to {@code advance(reuse)} or an internal object if reuse was null or incompatible.
             * Throws IllegalStateException if advance() has not been called successfully first.
             */
            Record current();
            // NOTE: The decoder must set serializedLength in the Record after reading.
        }
    }

    public static boolean isValidLog(final FileSystem fs, final Path path) throws IOException {
        long length = fs.getFileStatus(path).getLen();
        try (FSDataInputStream in = fs.open(path)) {
            if (LogTrailer.isValidTrailer(in, length)) {
                return true;
            } else {
              // Not a valid trailer, do we need to do something (set a flag)?
              // Fall back to checking the header.
              return LogHeader.isValidHeader(in);
            }
        }
    }

}
