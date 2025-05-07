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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the low-level reading of headers, blocks, and trailers for Log files. Manages reading
 * from the underlying stream, checksum validation, decompression, and providing access to block
 * data for the Codec.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class LogFileFormatReader implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileFormatReader.class);

    private LogFileReaderContext context;
    private LogFile.Codec.Decoder decoder;
    private SeekableDataInput input;
    private LogFile.Header header;
    private LogFile.Trailer trailer;
    private long currentPosition;
    private ByteBuffer currentBlockBuffer;
    private long currentBlockDataBytes;
    private long currentBlockConsumedBytes;
    private boolean trailerValidated;
    private CRC64 crc = new CRC64();

    public LogFileFormatReader() {

    }

    public void init(LogFileReaderContext context, SeekableDataInput input) throws IOException {
        this.context = context;
        this.input = input;
        this.currentBlockDataBytes = -1;
        this.currentBlockConsumedBytes = 0;
        try {
            readAndValidateTrailer();
            trailerValidated = true;
        } catch (IOException e) {
            // Log warning, trailer might be missing or corrupt, proceed without it
            LOG.warn("Failed to read or validate Log trailer for path: "
                + (context != null ? context.getFilePath() : "unknown")
                + ". Proceeding without trailer.", e);
            trailer = null; // Ensure trailer is null if reading/validation failed
        }
        this.decoder = null;
        // Seek to start of the file and read the header
        readHeader();
        currentPosition = input.getPos(); // Should be the offset of the first block
    }

    private void readAndValidateTrailer() throws IOException {
        if (context.getFileSize() < LogFileTrailer.FIXED_TRAILER_SIZE) {
            throw new IOException("File size " + context.getFileSize()
                + " is smaller than the fixed trailer size " + LogFileTrailer.FIXED_TRAILER_SIZE);
        }
        LogFileTrailer ourTrailer = new LogFileTrailer();
        // Fixed trailer fields will be LogTrailer.FIXED_TRAILER_SIZE bytes back from end of file.
        input.seek(context.getFileSize() - LogFileTrailer.FIXED_TRAILER_SIZE);
        // Read fixed fields
        ourTrailer.readFixedFields(input);
        // Now read the variable length protobuf message if present
        input.seek(ourTrailer.getTrailerStartOffset());
        ourTrailer.readMetadata(input);
        trailer = ourTrailer;
    }

    private void readHeader() throws IOException {
        header = new LogFileHeader();
        // Seek to start of file
        input.seek(0);
        header.readFields(input);
    }

    public LogFile.Record next() throws IOException {
        while (true) {
            // Check if we need a new block. Either we haven't started yet or the current block
            // is fully consumed.
            if (decoder == null || currentBlockConsumedBytes >= currentBlockDataBytes) {
                currentBlockBuffer = readNextBlock();
                if (currentBlockBuffer == null) {
                    // End of file or unrecoverable error after skipping blocks
                    validateReadCounts(); // Validate counts if trailer was read
                    return null;
                }
                currentBlockConsumedBytes = 0;
                // The limit of the buffer will be the size of the data in the block.
                currentBlockDataBytes = currentBlockBuffer.limit();
                decoder = context.getCodec().getDecoder(currentBlockBuffer);
            }
            // Advance the codec.
            try {
                if (decoder.advance()) {
                    // We advanced the codec, process the record.
                    LogFile.Record record = decoder.current();
                    int recordSize = record.getSerializedLength();
                    if (recordSize <= 0) {
                        // The codec must set the serialized length correctly. If it didn't this is
                        // likely a codec implementation problem but we will consider it an
                        // indication of block corruption.
                        throw new IOException("Codec failed to set serialized length for record at"
                            + " position " + (currentPosition + currentBlockConsumedBytes));
                    }
                    currentBlockConsumedBytes += recordSize;
                    context.incrementRecordsRead();
                    return record;
                } else {
                    // We failed to advance the codec but know we have bytes left in the block.
                    throw new IOException("Decoder failed to advance at position "
                        + (currentPosition + currentBlockConsumedBytes));
                }
            } catch (IOException e) {
                String message =
                    String.format("Encountered corrupt block at offset %d for path: %s",
                        currentPosition + currentBlockConsumedBytes, context.getFilePath());
                LOG.warn(message, e);
                context.incrementCorruptBlocksSkipped();
                if (!context.isSkipCorruptBlocks()) {
                    throw new IOException(message, e);
                }
                // Resync from the start of the current block
                if (!resyncReader(currentPosition)) {
                    return null;
                }
                decoder = null; // Force reading the next block
                continue; // Retry reading from the next block
            }
        }
    }

    // Reads the next block header, payload, checksum. Validates checksum. Decompresses.
    // Returns the decompressed block buffer, or null if EOF or unrecoverable error.
    // Manages skipping corrupt blocks if configured.
    private ByteBuffer readNextBlock() throws IOException {
        while (currentPosition < getEndOfDataOffset()) {
            long blockStartOffset = currentPosition;
            ByteBuffer decompressedBuffer = null;
            LogBlockHeader blockHeader = new LogBlockHeader();
            try {
                // Read Header
                blockHeader.readFields(input);
                currentPosition = input.getPos(); // Position after block header

                // Read Payload
                int payloadSize = blockHeader.getCompressedDataSize();
                ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize);

                try {
                    input.readFully(payloadBuffer.array(), payloadBuffer.arrayOffset(),
                        payloadSize);
                    payloadBuffer.limit(payloadSize);
                    currentPosition += payloadSize;

                    // Read Checksum
                    long expectedChecksum = input.readLong();
                    currentPosition += Bytes.SIZEOF_LONG;

                    // Validate Checksum
                    crc.reset();
                    // Checksum is on the raw payload bytes
                    crc.update(payloadBuffer.array(), payloadBuffer.arrayOffset(), payloadSize);
                    long actualChecksum = crc.getValue();

                    if (expectedChecksum != actualChecksum) {
                        throw new IOException("Checksum mismatch for block at offset "
                            + blockStartOffset + ", expected: " + expectedChecksum + ", actual: "
                            + actualChecksum);
                    }

                    // Decompress if necessary
                    if (blockHeader.getDataCompression() != Compression.Algorithm.NONE) {
                        decompressedBuffer = decompressBlock(payloadBuffer, blockHeader);
                    } else {
                        decompressedBuffer = payloadBuffer;
                    }
                } finally {
                    payloadBuffer = null;
                }

                context.incrementBlocksRead();
                return decompressedBuffer; // Successfully read and processed the block
            } catch (IOException | IllegalArgumentException e) {
                context.incrementCorruptBlocksSkipped();
                LOG.warn("Encountered corrupt block at offset " + blockStartOffset + " for path: "
                    + context.getFilePath(), e);
                if (!context.isSkipCorruptBlocks()) {
                    decompressedBuffer = null;
                    throw new IOException("Failed to read block at offset " + blockStartOffset, e);
                }
                // Attempt to skip this block and find the next one
                LOG.warn("Skipping corrupt block and attempting to resync...");
                if (!resyncReader(blockStartOffset)) {
                    // Cannot resync, likely EOF or further corruption
                    decompressedBuffer = null;
                    return null;
                }
                // Continue the loop to read the next block after resync
            }
        }
        return null;
    }

    // Decompresses the payload buffer using the specified algorithm.
    // Manages obtaining/releasing decompressors.
    private ByteBuffer decompressBlock(ByteBuffer compressedBuffer, LogFile.BlockHeader header)
            throws IOException {
        ByteBuffer decompressedBuffer;
        Compression.Algorithm compression = header.getDataCompression();
        Decompressor decompressor = compression.getDecompressor();
        try {
            decompressedBuffer = ByteBuffer.allocate(header.getUncompressedDataSize());
            decompressor.reset();
            decompressor.setInput(compressedBuffer.array(), compressedBuffer.arrayOffset(),
                header.getCompressedDataSize());
            int decompressedSize =
                decompressor.decompress(decompressedBuffer.array(),
                    decompressedBuffer.arrayOffset(), header.getUncompressedDataSize());
            if (decompressedSize != header.getUncompressedDataSize()) {
                throw new IOException("Decompression size mismatch: expected="
                    + header.getUncompressedDataSize() + ", actual=" + decompressedSize);
            }
            decompressedBuffer.limit(decompressedSize);
            return decompressedBuffer;
        } finally {
            compression.returnDecompressor(decompressor);
        }
    }

    // Tries to find the start of the next valid block after corruption.
    private boolean resyncReader(long offset) throws IOException {
        long nextOffset = offset + 1; // Start searching after the point of failure
        long endOfDataOffset = getEndOfDataOffset();
        while (nextOffset < endOfDataOffset) {
            long startPos = seekToMagic(nextOffset, LogBlockHeader.MAGIC);
            if (startPos < 0) {
                LOG.warn("Could not find next block magic after offset {}", nextOffset);
                return false; // EOF reached without finding magic bytes
            }
            // Found what look like magic bytes, now validate the header
            try {
                input.seek(startPos);
                LogBlockHeader blockHeader = new LogBlockHeader();
                blockHeader.readFields(input); // This reads exactly HEADER_SIZE bytes
                // Basic validation (readFields already checks magic and version)
                if (blockHeader.getUncompressedDataSize() < 0
                        || blockHeader.getCompressedDataSize() < 0) {
                    throw new IOException("Invalid block header found at offset " + startPos);
                }
                // Check if the block fits within the data boundary
                long blockEndOffset = startPos + blockHeader.getSerializedHeaderLength()
                    + blockHeader.getCompressedDataSize() + Bytes.SIZEOF_LONG;
                if (blockEndOffset > endOfDataOffset) {
                    throw new IOException("Possible block at offset " + startPos
                        + " extends beyond end of data offset " + endOfDataOffset);
                }
                // If we reached here, the header seems structurally valid.
                LOG.warn("Found valid block header at offset {}", startPos);
                input.seek(startPos);
                currentPosition = startPos;
                // Invalidate current decoder
                this.decoder = null;
                return true;
            } catch (EOFException e) {
                // Found magic bytes too close to the end of the file
                LOG.warn("Found magic bytes at offset {} but hit EOF trying to read header",
                    startPos);
                return false;
            } catch (IOException | IllegalArgumentException e) {
                // Header was invalid (bad magic, version, compression, sizes, etc.)
                LOG.warn("Found magic bytes at offset {} but header validation failed: {},"
                    + " continuing", startPos, e.getMessage());
                nextOffset = startPos + 1; // Continue searching after the magic bytes
            }
        }
        // Reached end of data without finding a valid block
        LOG.warn("Reached of data offset {} without finding a valid block", endOfDataOffset);
        return false;
    }

    // Helper to seek to the next occurrence of a magic byte sequence
    private long seekToMagic(long startOffset, byte[] magic) throws IOException {
        input.seek(startOffset);
        byte[] buffer = new byte[8192]; // Read in chunks, 8K seems reasonable. (Should be larger?)
        int magicPos = 0;
        while (true) {
            int bytesRead = input.read(buffer);
            if (bytesRead == -1) {
                return -1; // EOF
            }
            // This is brute force, is there something more efficient?
            for (int i = 0; i < bytesRead; i++) {
                if (buffer[i] == magic[magicPos]) {
                    magicPos++;
                    if (magicPos == magic.length) {
                        // Found block magic, position stream right before it.
                        long posAfterMagic = startOffset + (i + 1);
                        long magicStartPos = posAfterMagic - magic.length;
                        return magicStartPos;
                    }
                } else {
                    // Restart the match
                    magicPos = 0;
                    if (buffer[i] == magic[0]) {
                        magicPos = 1;
                    }
                }
            }
            startOffset += bytesRead;
        }
    }

    // Returns the offset where data blocks end, either EOF or start of trailer.
    private long getEndOfDataOffset() throws IOException {
        return trailer != null ? trailer.getTrailerStartOffset() : context.getFileSize();
    }

    // Validates read counts against trailer counts if trailer was successfully read
    private void validateReadCounts() {
        if (!trailerValidated || trailer == null) {
            return;
        }
        if (trailer.getBlockCount() != context.getBlocksRead()) {
            LOG.warn("Trailer block count mismatch! expected=" + trailer.getBlockCount()
                + ", actual=" + context.getBlocksRead());
        }
        if (trailer.getRecordCount() != context.getRecordsRead()) {
            LOG.warn("Trailer record count mismatch! expected=" + trailer.getRecordCount()
                + ", actual=" + context.getRecordsRead());
        }
    }

    @Override
    public void close() throws IOException {
        currentBlockBuffer = null;
        if (input != null) {
            try {
                input.close();
            } finally {
                input = null;
            }
        }
    }

    @Override
    public String toString() {
        return "LogFileFormatReader [context=" + context + ", decoder=" + decoder + ", input="
            + input + ", header=" + header + ", trailer=" + trailer + ", currentPosition="
            + currentPosition + ", currentBlockBuffer=" + currentBlockBuffer
            + ", currentBlockUncompressedSize=" + currentBlockDataBytes
            + ", currentBlockConsumedBytes=" + currentBlockConsumedBytes
            + ", trailerValidated=" + trailerValidated + "]";
    }

    LogFile.Header getHeader() {
        return header;
    }

    LogFile.Trailer getTrailer() {
        return trailer;
    }

}
