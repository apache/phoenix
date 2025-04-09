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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.phoenix.replication.util.CRC64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the low-level writing of headers, blocks, and trailers for Log files.
 * Manages buffering, compression, checksums, and writing to the underlying stream.
 */
public class LogFormatWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LogFormatWriter.class);

    private LogWriterContext writerContext;
    private Log.Codec.Encoder recordEncoder;
    private FSDataOutputStream outputStream;
    private Compressor compressor; // Reused per file
    private ByteArrayOutputStream currentBlockUncompressedBytes;
    private DataOutputStream blockDataStream;
    private boolean headerWritten = false;
    private boolean trailerWritten = false;
    private long recordCount = 0;
    private long blockCount = 0;
    private long blocksStartOffset = -1;
    private CRC64 crc = new CRC64(); // Indirect this when we have more than one type

    public LogFormatWriter() {

    }

    public void init(LogWriterContext context, FSDataOutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        this.writerContext = context;
        this.compressor = context.getCompression().getCompressor();
        this.currentBlockUncompressedBytes = new ByteArrayOutputStream();
        this.blockDataStream = new DataOutputStream(currentBlockUncompressedBytes);
        this.recordEncoder = context.getCodec().getEncoder(blockDataStream);
    }

    private void writeFileHeader() throws IOException {
        if (!headerWritten) {
            LogHeader header = new LogHeader();
            header.write(outputStream);
            blocksStartOffset = outputStream.getPos(); // First block starts after header
            headerWritten = true;
        }
    }

    public long getBlocksStartOffset() {
        return blocksStartOffset;
    }

    public void append(Log.Record record) throws IOException {
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
        recordEncoder.write(record);
        recordCount++;

        // Check if the current block size exceeds the limit AFTER writing the record
        if (currentBlockUncompressedBytes.size() >= writerContext.getMaxBlockSize()) {
            // To close the block, we do a sync(), which not only closes the block and opens a
            // new one, it syncs the finalized block.
            sync();
        }
    }

    // Should be called before writing the first record.
    public void startBlock() throws IOException {
        if (blockDataStream == null) {
          this.currentBlockUncompressedBytes = new ByteArrayOutputStream();
            this.blockDataStream = new DataOutputStream(currentBlockUncompressedBytes);
            // Re-initialize encoder for the new stream if necessary. This depends on the codec
            // implementation details. For now we assume it is necessary.
            this.recordEncoder = writerContext.getCodec().getEncoder(blockDataStream);
       }
    }

    // Closes the current block being written, compresses it (if applicable),
    // calculates checksum, and writes the block (header, payload, checksum) to the output stream.
    public void closeBlock() throws IOException {
        if (blockDataStream == null || currentBlockUncompressedBytes.size() == 0) {
            return; // No active block or block is empty
        }
        blockDataStream.flush(); // Ensure all encoded records are in the byte array
        byte[] uncompressedBytes = currentBlockUncompressedBytes.toByteArray();
        byte[] bytesToWrite;
        Compression.Algorithm ourCompression = writerContext.getCompression();
        if (compressor != null) {
            compressor.reset();
            ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
            try (DataOutputStream compressingStream = new DataOutputStream(
              ourCompression.createCompressionStream(compressedStream, compressor, 0))) {
                 compressingStream.write(uncompressedBytes);
            }
            bytesToWrite = compressedStream.toByteArray();
        } else {
            bytesToWrite = uncompressedBytes;
            ourCompression = Compression.Algorithm.NONE; // Explicitly NONE if no compressor
        }

        // Write block header
        Log.BlockHeader blockHeader = new LogBlockHeader()
            .setCompression(ourCompression)
            .setUncompressedSize(uncompressedBytes.length)
            .setCompressedSize(bytesToWrite.length);
        blockHeader.write(outputStream);

        outputStream.write(bytesToWrite);
        // Calculate checksum on the payload
        crc.reset();
        crc.update(bytesToWrite, 0, bytesToWrite.length);
        long checksum = crc.getValue();
        outputStream.writeLong(checksum); // Write CRC64 of header and payload

        blockCount++;

        // Reset for the next block
        // blockDataStream remains wrapping the reset currentBlockUncompressedBytes
        currentBlockUncompressedBytes.reset();
        blockDataStream = null;
    }

    public void sync() throws IOException {
        // Ensure the current block data is flushed to the FSDataOutputStream.
        if (blockDataStream != null && currentBlockUncompressedBytes.size() > 0) {
          // Closing the current block forces its header, data (potentially compressed),
          // and checksum into the outputStream buffer.
          closeBlock();
          // Start a new block for subsequent appends.
          startBlock();
        }
        // Sync the underlying FSDataOutputStream.
        outputStream.hsync();
    }

    public long getPosition() throws IOException {
        return outputStream.getPos();
    }

    @Override
    public void close() throws IOException {
        if (trailerWritten) {
            return;
        }
        try {
            if (!headerWritten) {
                // We might be closing an empty file
                writeFileHeader();
            }
            // Close any outstanding block first
            closeBlock();
            writeTrailer();
        } finally {
            IOUtils.closeQuietly(outputStream);
            if (compressor != null) {
                writerContext.getCompression().returnCompressor(compressor);
                compressor = null;
            }
        }
    }

    private void writeTrailer() throws IOException {
        Log.Trailer trailer = new LogTrailer()
            .setRecordCount(recordCount)
            .setBlockCount(blockCount)
            .setBlocksStartOffset(blocksStartOffset)
            .setTrailerStartOffset(outputStream.getPos());
        trailer.write(outputStream);
        trailerWritten = true;
        try {
            outputStream.hsync();
        } catch (IOException e) {
            // Failed sync on trailer write isn't a fatal event.
            LOG.warn("Exception while syncing Log trailer", e);
        }
    }

    @Override
    public String toString() {
        return "LogFormatWriter [writerContext=" + writerContext
            + ", currentBlockUncompressedBytes=" + currentBlockUncompressedBytes
            + ", headerWritten=" + headerWritten + ", trailerWritten=" + trailerWritten
            + ", recordCount=" + recordCount + ", blockCount=" + blockCount
            + ", blocksStartOffset=" + blocksStartOffset + "]";
    }

}
