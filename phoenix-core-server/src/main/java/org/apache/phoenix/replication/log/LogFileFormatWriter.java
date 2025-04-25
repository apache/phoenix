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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.phoenix.replication.util.CRC64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the low-level writing of headers, blocks, and trailers for Log files.
 * Manages buffering, compression, checksums, and writing to the underlying stream.
 */
public class LogFileFormatWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileFormatWriter.class);

    private LogFileWriterContext context;
    private LogFile.Codec.Encoder encoder;
    private SyncableDataOutput output;
    private ByteArrayOutputStream currentBlockBytes;
    private DataOutputStream blockDataStream;
    private boolean headerWritten = false;
    private boolean trailerWritten = false;
    private long recordCount = 0;
    private long blockCount = 0;
    private long blocksStartOffset = -1;
    private CRC64 crc = new CRC64(); // Indirect this when we have more than one type
    // Cached buffer for compression for performance
    ByteBuffer compressBuff = null;

    public LogFileFormatWriter() {

    }

    public void init(LogFileWriterContext context, HDFSDataOutput output) throws IOException {
        this.output = output;
        this.context = context;
        this.currentBlockBytes = new ByteArrayOutputStream();
        this.blockDataStream = new DataOutputStream(currentBlockBytes);
        this.encoder = context.getCodec().getEncoder(blockDataStream);
    }

    private void writeFileHeader() throws IOException {
        if (!headerWritten) {
            LogFileHeader header = new LogFileHeader();
            header.write(output);
            blocksStartOffset = output.getPos(); // First block starts after header
            headerWritten = true;
        }
    }

    public long getBlocksStartOffset() {
        return blocksStartOffset;
    }

    public void append(LogFile.Record record) throws IOException {
        if (!headerWritten) {
            // Lazily write file header
            writeFileHeader();
        }
        if (trailerWritten) {
            throw new IOException("Cannot append record after trailer has been written");
        }
        if (blockDataStream == null) {
            startBlock(); // Start the block if needed
        }
        encoder.write(record);
        recordCount++;

        // Check if the current block size exceeds the limit AFTER writing the record
        if (currentBlockBytes.size() >= context.getMaxBlockSize()) {
            // To close the block, we do a sync(), which not only closes the block and opens a
            // new one, it syncs the finalized block.
            sync();
        }
    }

    // Should be called before writing the first record.
    public void startBlock() throws IOException {
        if (blockDataStream == null) {
            this.currentBlockBytes = new ByteArrayOutputStream();
            this.blockDataStream = new DataOutputStream(currentBlockBytes);
            // Re-initialize encoder for the new stream if necessary. This depends on the codec
            // implementation details. For now we assume it is necessary.
            this.encoder = context.getCodec().getEncoder(blockDataStream);
        }
    }

    // Closes the current block being written, compresses it (if applicable),
    // calculates checksum, and writes the block (header, payload, checksum) to the output stream.
    public void closeBlock() throws IOException {
        if (blockDataStream == null || currentBlockBytes.size() == 0) {
            return; // No active block or block is empty
        }
        blockDataStream.flush(); // Ensure all encoded records are in the byte array
        byte[] uncompressedBytes = currentBlockBytes.toByteArray();
        ByteBuffer writeBuff;
        int lengthToWrite;
        Compression.Algorithm ourCompression = context.getCompression();
        if (ourCompression != Compression.Algorithm.NONE) {
            Compressor compressor = ourCompression.getCompressor();
            try {
                compressor.reset();
                compressor.setInput(uncompressedBytes, 0, uncompressedBytes.length);
                compressor.finish(); // We are going to one-shot this.
                // Give 20% overhead for pathological cases
                // We can't go below this by much because the Snappy compressor will require more than
                // 10% overhead or else it will refuse to try.
                int compressBuffNeeded = (int)(uncompressedBytes.length * 1.2f);
                if (compressBuff == null || compressBuff.capacity() < compressBuffNeeded) {
                    compressBuff = ByteBuffer.allocate(compressBuffNeeded);
                }
                compressBuff.clear();
                lengthToWrite = compressor.compress(compressBuff.array(), compressBuff.arrayOffset(),
                    compressBuffNeeded);
                if (!compressor.finished()) {
                    throw new IOException("Compressor did not finish");
                }
                writeBuff = compressBuff;
            } finally {
                context.getCompression().returnCompressor(compressor);;
            }
        } else {
            writeBuff = ByteBuffer.wrap(uncompressedBytes);
            lengthToWrite = uncompressedBytes.length;
        }

        // Write block header
        LogFile.BlockHeader blockHeader = new LogBlockHeader()
            .setDataCompression(ourCompression)
            .setUncompressedDataSize(uncompressedBytes.length)
            .setCompressedDataSize(lengthToWrite);
        blockHeader.write(output);

        output.write(writeBuff.array(), writeBuff.arrayOffset(), lengthToWrite);
        // Calculate checksum on the payload
        crc.reset();
        crc.update(writeBuff.array(), writeBuff.arrayOffset(), lengthToWrite);
        long checksum = crc.getValue();
        output.writeLong(checksum); // Write CRC64 of header and payload

        blockCount++;

        // Reset for the next block
        // blockDataStream remains wrapping the reset currentBlockUncompressedBytes
        currentBlockBytes.reset();
        blockDataStream = null;
    }

    public void sync() throws IOException {
        // Ensure the current block data is flushed to the FSDataOutputStream.
        if (blockDataStream != null && currentBlockBytes.size() > 0) {
            // Closing the current block forces its header, data (potentially compressed),
            // and checksum into the outputStream buffer.
            closeBlock();
            // Flush and sync the underlying output
            output.sync();
            // Start a new block for subsequent appends.
            startBlock();
        }
    }

    public long getPosition() throws IOException {
        return output.getPos();
    }

    @Override
    public void close() throws IOException {
        // We use the fact we have already written the trailer as the boolean "closed" condition.
        if (trailerWritten) {
            return;
        }
        try {
            // We might be closing an empty file, handle this case correctly.
            if (!headerWritten) {
                writeFileHeader();
            }
            // Close any outstanding block.
            closeBlock();
            // After we write the trailer we consider the file closed.
            writeTrailer();
        } finally {
            if (output != null) {
                // We need to catch the exception in order to prevent a compressor leak.
                try {
                    output.close();
                } catch (IOException e) {
                    LOG.error("Exception while closing LogFormatWriter", e);
                }
            }
        }
    }

    private void writeTrailer() throws IOException {
        LogFile.Trailer trailer = new LogFileTrailer()
            .setRecordCount(recordCount)
            .setBlockCount(blockCount)
            .setBlocksStartOffset(blocksStartOffset)
            .setTrailerStartOffset(output.getPos());
        trailer.write(output);
        trailerWritten = true;
        try {
            output.sync();
        } catch (IOException e) {
            // Failed sync on trailer write isn't a fatal event.
            LOG.warn("Exception while syncing Log trailer", e);
        }
    }

    @Override
    public String toString() {
        return "LogFileFormatWriter [writerContext=" + context
            + ", currentBlockUncompressedBytes=" + currentBlockBytes
            + ", headerWritten=" + headerWritten + ", trailerWritten=" + trailerWritten
            + ", recordCount=" + recordCount + ", blockCount=" + blockCount
            + ", blocksStartOffset=" + blocksStartOffset + "]";
    }

}
