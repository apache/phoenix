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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.compress.Compression;

/**
 * Defines the structure and constants for Phoenix Replication Log files.
 * Provides interfaces for reading and writing these logs.
 */
public interface LogFile {

    /** Represents the file header */
    interface Header {
        /**
         * Gets the major version of the log file format.
         * @return The major version number.
         */
        int getMajorVersion();

        /**
         * Sets the major version of the log file format.
         * @param majorVersion The major version number to set.
         * @return This Header instance for chaining.
         */
        Header setMajorVersion(int majorVersion);

        /**
         * Gets the minor version of the log file format.
         * @return The minor version number.
         */
        int getMinorVersion();

        /**
         * Sets the minor version of the log file format.
         * @param minorVersion The minor version number to set.
         * @return This Header instance for chaining.
         */
        Header setMinorVersion(int minorVersion);

        /**
         * Gets the fixed serialized length of the header in bytes.
         * @return The length of the serialized header.
         */
        int getSerializedLength();

        /**
         * Reads the header fields from the provided DataInput stream.
         * @param in The DataInput stream to read from.
         * @throws IOException if an I/O error occurs or the header format is invalid.
         */
        void readFields(DataInput in) throws IOException;

        /**
         * Writes the header fields to the provided DataOutput stream.
         * @param out The DataOutput stream to write to.
         * @throws IOException if an I/O error occurs.
         */
        void write(DataOutput out) throws IOException;
    }

    /** Represents the header of a single block within the log file */
    interface BlockHeader {
        /**
         * Gets the version of this block header format.
         * @return The block header version number.
         */
        int getVersion();

        /**
         * Gets the compression algorithm used for the data payload of this block.
         * @return The compression algorithm.
         */
        Compression.Algorithm getDataCompression();

        /**
         * Sets the compression algorithm used for the data payload of this block.
         * @param compression The compression algorithm to set.
         * @return This BlockHeader instance for chaining.
         */
        BlockHeader setDataCompression(Compression.Algorithm compression);

        /**
         * Gets the size of the data payload before compression.
         * @return The uncompressed data size in bytes.
         */
        int getUncompressedDataSize();

        /**
         * Sets the size of the data payload before compression.
         * @param uncompressedSize The uncompressed data size in bytes.
         * @return This BlockHeader instance for chaining.
         */
        BlockHeader setUncompressedDataSize(int uncompressedSize);

        /**
         * Gets the size of the data payload after compression. If compression is NONE,
         * this will be the same as the uncompressed size.
         * @return The compressed data size in bytes.
         */
        int getCompressedDataSize();

        /**
         * Sets the size of the data payload after compression.
         * @param compressedSize The compressed data size in bytes.
         * @return This BlockHeader instance for chaining.
         */
        BlockHeader setCompressedDataSize(int compressedSize);

        /**
         * Gets the fixed serialized length of the block header itself in bytes.
         * @return The length of the serialized block header.
         */
        int getSerializedHeaderLength();

        /**
         * Reads the block header fields from the provided DataInput stream.
         * @param in The DataInput stream to read from.
         * @throws IOException if an I/O error occurs or the block header format is invalid.
         */
        void readFields(DataInput in) throws IOException;

        /**
         * Writes the block header fields to the provided DataOutput stream.
         * @param out The DataOutput stream to write to.
         * @throws IOException if an I/O error occurs.
         */
        void write(DataOutput out) throws IOException;
    }

    /** Represents the file trailer */
    interface Trailer {
        /**
         * Gets the major version of the log file format stored in the trailer.
         * Useful for validation when reading from the end of the file.
         * @return The major version number.
         */
        int getMajorVersion();

        /**
         * Sets the major version of the log file format in the trailer.
         * @param majorVersion The major version number to set.
         * @return This Trailer instance for chaining.
         */
        Trailer setMajorVersion(int majorVersion);

        /**
         * Gets the minor version of the log file format stored in the trailer.
         * Useful for validation when reading from the end of the file.
         * @return The minor version number.
         */
        int getMinorVersion();

        /**
         * Sets the minor version of the log file format in the trailer.
         * @param minorVersion The minor version number to set.
         * @return This Trailer instance for chaining.
         */
        Trailer setMinorVersion(int minorVersion);

        /**
         * Gets the total number of records contained in the log file.
         * @return The total record count.
         */
        long getRecordCount();

        /**
         * Sets the total number of records contained in the log file.
         * @param recordCount The total record count to set.
         * @return This Trailer instance for chaining.
         */
        Trailer setRecordCount(long recordCount);

        /**
         * Gets the total number of blocks contained in the log file.
         * @return The total block count.
         */
        long getBlockCount();

        /**
         * Sets the total number of blocks contained in the log file.
         * @param blockCount The total block count to set.
         * @return This Trailer instance for chaining.
         */
        Trailer setBlockCount(long blockCount);

        /**
         * Gets the byte offset within the file where the first block begins (after the header).
         * @return The starting offset of the first block.
         */
        long getBlocksStartOffset();

        /**
         * Sets the byte offset within the file where the first block begins.
         * @param offset The starting offset of the first block.
         * @return This Trailer instance for chaining.
         */
        Trailer setBlocksStartOffset(long offset);

        /**
         * Gets the byte offset within the file where the trailer itself begins.
         * @return The starting offset of the trailer.
         */
        long getTrailerStartOffset();

        /**
         * Sets the byte offset within the file where the trailer itself begins.
         * @param offset The starting offset of the trailer.
         * @return This Trailer instance for chaining.
         */
        Trailer setTrailerStartOffset(long offset);

        /**
         * Gets the total serialized length of the trailer in bytes, including any variable-length
         * metadata.
         * @return The total length of the serialized trailer.
         */
        int getSerializedLength();

        /**
         * Reads the trailer fields from the provided DataInput stream. Assumes the stream
         * is positioned at the start of the trailer.
         * @param in The DataInput stream to read from.
         * @throws IOException if an I/O error occurs or the trailer format is invalid.
         */
        void readFields(DataInput in) throws IOException;

        /**
         * Writes the trailer fields to the provided DataOutput stream.
         * @param out The DataOutput stream to write to.
         * @throws IOException if an I/O error occurs.
         */
        void write(DataOutput out) throws IOException;
    }

    /** Represents a single logical change */
    interface Record {
        /**
         * Gets the mutation this record represents.
         * @return The Mutation.
         */
        Mutation getMutation();

        /**
         * Sets the mutation this record represents.
         * @param mutation The Mutation to set.
         * @return This Record instance for chaining.
         */
        Record setMutation(Mutation mutation);

        /**
         * Gets the name of the HBase table this record pertains to.
         * @return The HBase table name.
         */
        String getHBaseTableName();

        /**
         * Sets the name of the HBase table this record pertains to.
         * @param tableName The HBase table name to set.
         * @return This Record instance for chaining.
         */
        Record setHBaseTableName(String tableName);

        /**
         * Gets the commit ID or System Change Number (SCN) associated with this record.
         * @return The commit ID/SCN.
         */
        long getCommitId();

        /**
         * Sets the commit ID or System Change Number (SCN) for this record.
         * @param commitId The commit ID/SCN to set.
         * @return This Record instance for chaining.
         */
        Record setCommitId(long commitId);

        /**
         * Gets the total serialized length of this record in bytes, including any length prefixes
         * used by the codec. This value should be set by the codec after writing or reading.
         * @return The total serialized length of the record.
         */
        int getSerializedLength();

        /**
         * Sets the total serialized length of this record. This is typically called by the codec.
         * @param serializedLength The total serialized length in bytes.
         * @return This Record instance for chaining.
         */
        Record setSerializedLength(int serializedLength);
    }

    /** Interface for writing replication logs */
    interface Writer extends Closeable {
        /**
         * Initializes the writer with necessary context.
         * @param context The context containing initialization parameters.
         * @throws IOException if initialization fails.
         */
        void init(LogFileWriterContext context) throws IOException;

        /**
         * Appends an HBase mutation to the log file. The log record may be buffered internally.
         * @param tableName The HBase table name
         * @param commitId The commit identifier
         * @param mutation The mutation to append.
         * @throws IOException if an I/O error occurs during append.
         */
        void append(String tableName, long commitId, Mutation mutation) throws IOException;

        /**
         * Flushes any buffered data to the underlying storage and ensures it is durable
         * (e.g., by calling hsync on the FSDataOutputStream). This guarantees that records
         * appended before the sync are persisted.
         * @throws IOException if an I/O error occurs during sync.
         */
        void sync() throws IOException;

        /**
         * Returns the current length of the log file being written, in bytes.
         * This typically reflects the position of the underlying output stream.
         * @return The current file length.
         * @throws IOException if the length cannot be determined.
         */
        long getLength() throws IOException;
    }

    /** Interface for reading replication logs */
    interface Reader extends Closeable, Iterable<Record> {
        /**
         * Initializes the reader with necessary context.
         * This typically involves opening the file and reading the header.
         * @param context The context containing initialization parameters.
         * @throws IOException if initialization fails (e.g., file not found, invalid header).
         */
        void init(LogFileReaderContext context) throws IOException;

        /**
         * Reads and returns the next record from the log file, as an HBase mutation.
         * @return The next Record, or null if the end of the file has been reached.
         * @throws IOException if an I/O error occurs during reading or parsing.
         */
        Record next() throws IOException;

        /**
         * Returns the Header information read from the log file during initialization.
         * @return The file Header.
         * @throws IllegalStateException if the reader has not been initialized.
         */
        Header getHeader();

        /**
         * Returns the Trailer information read from the log file during initialization.
         * If the trailer was missing or corrupt, this may return null.
         * @return The file Trailer, or null if not present or invalid.
         * @throws IllegalStateException if the reader has not been initialized.
         */
        Trailer getTrailer();
    }

    /** Interface for encoding/decoding Records within a block's buffer */
    interface Codec {
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
             * @return true if a record was successfully read, false if the end of the stream
             *         is reached.
             * @throws IOException if an error occurs during reading.
             */
            boolean advance() throws IOException;
            /**
             * Returns the current record, which might be the object passed in the last successful
             * call to {@code advance(reuse)} or an internal object if reuse was null.
             * Throws IllegalStateException if advance() has not been called successfully first.
             */
            Record current();
            // NOTE: The decoder must set serializedLength in the Record after reading.
        }
    }

    /**
     * Utility for determining if a file is a valid replication log file.
     * @param fs The FileSystem
     * @param path Path to the potential replication log file
     * @return true if the file is a valid replication log file, false otherwise
     * @throws IOException if an I/O problem was encountered
     */
    static boolean isValidLogFile(final FileSystem fs, final Path path) throws IOException {
        long length = fs.getFileStatus(path).getLen();
        try (FSDataInputStream in = fs.open(path)) {
            if (LogFileTrailer.isValidTrailer(in, length)) {
                return true;
            } else {
                // Not a valid trailer, do we need to do something (set a flag)?
                // Fall back to checking the header.
                return LogFileHeader.isValidHeader(in);
            }
        }
    }

}
