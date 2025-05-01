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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileFormatTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileFormatTest.class);

    private Configuration conf;
    private ByteArrayOutputStream writerBaos;
    private DataOutputStream writerDos;
    private LogFileReaderContext readerContext;
    private LogFileFormatReader reader;
    private LogFileWriterContext writerContext;
    private LogFileFormatWriter writer;

    @Before
    public void setUp() {
        conf = HBaseConfiguration.create();
        readerContext = new LogFileReaderContext(conf)
            .setSkipCorruptBlocks(true); // Enable skipping for corruption tests
        reader = new LogFileFormatReader();
        writerBaos = new ByteArrayOutputStream();
        writerDos = new DataOutputStream(writerBaos);
        writerContext = new LogFileWriterContext(conf);
        writer = new LogFileFormatWriter();
    }

    @After
    public void tearDown() throws IOException {
        writerDos.close();
        writerBaos.close();
        writer.close();
        reader.close();
    }

    @Test
    public void testLogFileFormatSingleBlock() throws IOException {
        initLogFileWriter();
        LogFile.Record r1 = LogFileTestUtil.newPutRecord("TBL1", 1L, "row1", 10L, 1);
        LogFile.Record r2 = LogFileTestUtil.newPutRecord("TBL1", 2L, "row2", 11L, 1);

        writer.append(r1);
        writer.append(r2);
        writer.close(); // Writes block and trailer

        byte[] data = writerBaos.toByteArray();
        initLogFileReader(data);

        assertEquals("Major version mismatch", LogFileHeader.VERSION_MAJOR,
            reader.getHeader().getMajorVersion());
        assertEquals("Minor version mismatch", LogFileHeader.VERSION_MINOR,
            reader.getHeader().getMinorVersion());

        LogFile.Record decoded1 = reader.next();
        assertNotNull("First record should not be null", decoded1);
        LogFileTestUtil.assertRecordEquals("First record mismatch", r1, decoded1);

        LogFile.Record decoded2 = reader.next();
        assertNotNull("Second record should not be null", decoded2);
        LogFileTestUtil.assertRecordEquals("Second record mismatch", r2, decoded2);

        assertNull("Should be no more records", reader.next());

        LogFile.Trailer trailer = reader.getTrailer();
        assertNotNull("Trailer should exist", trailer);
        assertEquals("Trailer record count mismatch", 2, trailer.getRecordCount());
        assertEquals("Trailer block count mismatch", 1, trailer.getBlockCount());
        assertTrue("Blocks start offset should be positive", trailer.getBlocksStartOffset() > 0);
        assertTrue("Trailer start offset should be > blocks start offset",
            trailer.getTrailerStartOffset() >= trailer.getBlocksStartOffset());

        assertEquals("Reader context record count mismatch", 2, readerContext.getRecordsRead());
        assertEquals("Reader context block count mismatch", 1, readerContext.getBlocksRead());
    }

    @Test
    public void testLogFileFormatMultipleBlocks() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> originals = new ArrayList<>();
        // Write enough records to cause multiple blocks
        // This has the nice property of writing a large number of blocks compared with the other
        // tests in this unit.
        for (int i = 0; i < 100_000; i++) {
            LogFile.Record r = LogFileTestUtil.newPutRecord("TBLMULTI", i, "row" + i, 10L + i, 2);
            originals.add(r);
            writer.append(r);
        }
        writer.close();

        byte[] data = writerBaos.toByteArray();
        initLogFileReader(data);

        List<LogFile.Record> decoded = new ArrayList<>();
        LogFile.Record r;
        while ((r = reader.next()) != null) {
            decoded.add(r);
        }

        assertEquals("Number of records mismatch", originals.size(), decoded.size());
        for (int i = 0; i < originals.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Record " + i + " mismatch", originals.get(i),
                decoded.get(i));
        }

        LogFile.Trailer trailer = reader.getTrailer();
        assertNotNull("Trailer should exist", trailer);
        assertEquals("Trailer record count mismatch", originals.size(), trailer.getRecordCount());
        assertTrue("Trailer block count should be > 1", trailer.getBlockCount() > 1);
        assertEquals("Reader context record count mismatch", originals.size(),
            readerContext.getRecordsRead());
        assertEquals("Reader context block count mismatch", trailer.getBlockCount(),
            readerContext.getBlocksRead());
    }

    @Test
    public void testLogFileFormatHeaderTrailerOnly() throws IOException {
        // Write header and trailer, no blocks
        initLogFileWriter();
        writer.close();
        byte[] data = writerBaos.toByteArray();
        initLogFileReader(data);
        assertEquals("Major version mismatch", LogFileHeader.VERSION_MAJOR,
            reader.getHeader().getMajorVersion());
        assertNull("Should be no records", reader.next());
        LogFileTrailer trailer = (LogFileTrailer) reader.getTrailer();
        assertNotNull("Trailer should exist", trailer);
        assertEquals("Trailer record count should be 0", 0, trailer.getRecordCount());
        assertEquals("Trailer block count should be 0", 0, trailer.getBlockCount());
        // blocksStartOffset might be equal to trailerStartOffset if no blocks written
        assertEquals("Blocks start offset should equal trailer start offset",
            trailer.getBlocksStartOffset(), trailer.getTrailerStartOffset());
    }

    @Test
    public void testLogFileCorruptionInvalidBlockMagic() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> block1Records = writeBlock(writer, "B1",  0, 10);
        List<LogFile.Record> block2Records = writeBlock(writer, "B2", 10, 10);
        List<LogFile.Record> block3Records = writeBlock(writer, "B3", 20, 10);
        writer.close();

        byte[] data = writerBaos.toByteArray();

        int block1End = findBlockEndOffset(data, (int)writer.getBlocksStartOffset());
        data[(int)block1End] = (byte) 'X'; // Corrupt the first byte of 'PBLK'

        initLogFileReader(data);

        List<LogFile.Record> decoded = readRecords(reader);

        int shouldHave = block1Records.size() + block3Records.size();
        int have = decoded.size();
        // Should read block 1 and block 3, skipping block 2
        assertEquals("Should read records from block 1 and 3", shouldHave, have);
        // Verify first block records
        for (int i = 0; i < block1Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 1 record " + i + " mismatch",
                block1Records.get(i), decoded.get(i));
        }
        // Verify third block records (offset by block1 size)
        for (int i = 0; i < block3Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 3 record " + i + " mismatch",
                block3Records.get(i), decoded.get(i + block1Records.size()));
        }

        assertEquals("Should have skipped 1 corrupt block", 1,
            readerContext.getCorruptBlocksSkipped());
    }

    @Test
    public void testLogFileCorruptionBadBlockChecksum() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> block1Records = writeBlock(writer, "B1",  0, 5);
        List<LogFile.Record> block2Records = writeBlock(writer, "B2",  5, 5);
        List<LogFile.Record> block3Records = writeBlock(writer, "B3", 10, 5);
        writer.close();

        byte[] data = writerBaos.toByteArray();

        // Find offset of second block's checksum and corrupt it
        long block1End = findBlockEndOffset(data, (int)writer.getBlocksStartOffset());
        long block2End = findBlockEndOffset(data, (int)block1End);
        int block2ChecksumOffset = (int) (block2End - Bytes.SIZEOF_LONG);
        data[block2ChecksumOffset] ^= 0xFF; // Flip some bits in the checksum

        initLogFileReader(data);

        List<LogFile.Record> decoded = readRecords(reader);

        // Should read block 1 and block 3, skipping block 2
        int shouldHave = block1Records.size() + block3Records.size();
        int have = decoded.size();
        assertEquals("Should read records from block 1 and 3", shouldHave, have);
        // Verify first block records
        for (int i = 0; i < block1Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 1 record " + i + " mismatch",
                block1Records.get(i), decoded.get(i));
        }
        // Verify third block records (offset by block1 size)
        for (int i = 0; i < block3Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 3 record " + i + " mismatch",
                block3Records.get(i), decoded.get(i + block1Records.size()));
        }

        assertEquals("Should have skipped 1 corrupt block", 1,
            readerContext.getCorruptBlocksSkipped());
    }

    @Test
    public void testLogFileCorruptionBadRecord() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> block1Records = writeBlock(writer, "B1",  0, 10);
        List<LogFile.Record> block2Records = writeBlock(writer, "B2", 10, 10);
        List<LogFile.Record> block3Records = writeBlock(writer, "B3", 20, 10);
        writer.close();

        byte[] data = writerBaos.toByteArray();

        // Find offset of second block's first record and corrupt it
        long block1End = findBlockEndOffset(data, (int)writer.getBlocksStartOffset());
        long block2Start = block1End + LogBlockHeader.HEADER_SIZE;
        data[(int)block2Start + 1] ^= 0xFF;

        initLogFileReader(data);

        List<LogFile.Record> decoded = readRecords(reader);

        // Should read block 1 and block 3, skipping block 2
        int shouldHave = block1Records.size() + block3Records.size();
        int have = decoded.size();
        assertEquals("Should read records from block 1 and 3", shouldHave, have);
        // Verify first block records
        for (int i = 0; i < block1Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 1 record " + i + " mismatch",
                block1Records.get(i), decoded.get(i));
        }
        // Verify third block records (offset by block1 size)
        for (int i = 0; i < block3Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 3 record " + i + " mismatch",
                block3Records.get(i), decoded.get(i + block1Records.size()));
        }

        assertEquals("Should have skipped 1 corrupt block", 1,
            readerContext.getCorruptBlocksSkipped());
    }

    @Test
    public void testLogFileCorruptionTruncatedFinalBlock() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> block1Records = writeBlock(writer, "B1",  0, 10);
        List<LogFile.Record> block2Records = writeBlock(writer, "B2", 10, 10);
        // Don't close the writer, simulate truncation within the last block
        // Get the current position which is somewhere inside block 2's data
        long truncationPoint = writer.getPosition() - 10; // Truncate 10 bytes before end

        byte[] data = writerBaos.toByteArray();
        byte[] truncatedData = Arrays.copyOf(data, (int) truncationPoint);

        initLogFileReader(truncatedData);

        List<LogFile.Record> decoded = new ArrayList<>();
        LogFile.Record r;
        try {
            while ((r = reader.next()) != null) {
                decoded.add(r);
            }
            // Depending on where truncation happened, next() might throw or return null.
            // If it returns null cleanly, the counts should still be checked.
        } catch (IOException e) {
            // Expecting an EOF or similar if truncation happened mid-record/header/checksum
        }

        // Should have read only block 1 completely. Block 2 read might fail or be partial.
        assertEquals("Should read records only from block 1", block1Records.size(),
            decoded.size());
        for (int i = 0; i < block1Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 1 record " + i + " mismatch",
                block1Records.get(i), decoded.get(i));
        }

        // Depending on where the truncation happens, the block might be detected as corrupt or
        // just end early.
        assertTrue("Corrupt blocks skipped should be 0 or 1",
            readerContext.getCorruptBlocksSkipped() <= 1);
        assertEquals("Blocks read count should be 1 (Block 1)", 1, readerContext.getBlocksRead());
        assertEquals("Records read count mismatch", block1Records.size(),
            readerContext.getRecordsRead());
        assertNull("Trailer should not be present", reader.getTrailer());
    }

    @Test
    public void testLogFileCorruptionMissingTrailer() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> block1Records = writeBlock(writer, "B1", 0, 5);
        // Don't close the writer, simulate missing trailer
        long trailerStartOffset = writer.getPosition(); // Position before trailer write

        byte[] data = writerBaos.toByteArray();
        byte[] truncatedData = Arrays.copyOf(data, (int) trailerStartOffset);

        // Re-initialize reader with truncated data
        PositionedByteArrayInputStream input = new PositionedByteArrayInputStream(truncatedData);
        readerContext.setFileSize(truncatedData.length);
        // This init should log a warning but succeed
        reader.init(readerContext, input);

        List<LogFile.Record> decoded = readRecords(reader);

        assertEquals("Should read all records from block 1", block1Records.size(), decoded.size());
        for (int i = 0; i < block1Records.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Block 1 record " + i + " mismatch",
                block1Records.get(i), decoded.get(i));
        }

        assertNull("Trailer should be null", reader.getTrailer());
        assertEquals("Corrupt blocks skipped should be 0", 0,
            readerContext.getCorruptBlocksSkipped());
        assertEquals("Blocks read count mismatch", 1, readerContext.getBlocksRead());
        assertEquals("Records read count mismatch", block1Records.size(),
            readerContext.getRecordsRead());
    }

    static class PositionedByteArrayInputStream extends ByteArrayInputStream
            implements SeekableDataInput {

        // A view of ourselves as a data input stream
        private DataInputStream stream;

        public PositionedByteArrayInputStream(byte[] buf) {
            super(buf);
            this.stream = new DataInputStream(this);
        }

        public PositionedByteArrayInputStream(byte[] buf, int offset, int length) {
            super(buf, offset, length);
            this.stream = new DataInputStream(this);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0) {
                throw new IOException("Cannot seek to negative position");
            }
            if (pos > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Cannot seek beyond Integer.MAX_VALUE");
            }
            if (pos >= count) {
                this.pos = count; // Position at the end
                if (pos > count) {
                    throw new EOFException("Seek position is past the end of the stream");
                }
            } else {
                this.pos = (int) pos;
            }
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long pos) throws IOException {
          seek(pos);
          return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            if (buffer == null) {
                throw new NullPointerException();
            }
            if (offset < 0 || length < 0 || offset + length > buffer.length) {
                throw new IndexOutOfBoundsException(String.format(
                    "Offset %d or length %d is invalid for buffer of size %d", offset, length,
                        buffer.length));
            }
            if (position < 0) {
                throw new IOException("Position cannot be negative");
            }
            if (position > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Position cannot exceed Integer.MAX_VALUE");
            }
            if (length == 0) {
                return 0;
            }
            int pos = (int) position;
            if (pos >= count) {
                return -1;
            }
            int avail = count - pos;
            int n = Math.min(length, avail);
            System.arraycopy(buf, pos, buffer, offset, n);
            return n;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException {
            if (buffer == null) {
                throw new NullPointerException("Buffer cannot be null");
            }
            if (offset < 0 || length < 0 || offset + length > buffer.length) {
                throw new IndexOutOfBoundsException(String.format(
                    "Offset %d or length %d is invalid for buffer of size %d", offset, length,
                        buffer.length));
            }
            if (position < 0) {
                throw new IOException("Position cannot be negative");
            }
            if (position > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Position cannot exceed Integer.MAX_VALUE");
            }
            if (length == 0) {
                return;
            }
            int pos = (int) position;
            if (pos >= count) {
                throw new EOFException("Read position is at or past the end of the stream");
            }
            int avail = count - pos;
            if (length > avail) {
                throw new EOFException(String.format(
                    "Premature EOF from stream: expected %d bytes, but only %d available", length,
                        avail));
            }
            System.arraycopy(buf, pos, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            stream.readFully(b);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            stream.readFully(b, off, len);
        }

        @Override
        public int skipBytes(int n) throws IOException {
            return stream.skipBytes(n);
        }

        @Override
        public boolean readBoolean() throws IOException {
            return stream.readBoolean();
        }

        @Override
        public byte readByte() throws IOException {
            return stream.readByte();
        }

        @Override
        public int readUnsignedByte() throws IOException {
            return stream.readUnsignedByte();
        }

        @Override
        public short readShort() throws IOException {
            return stream.readShort();
        }

        @Override
        public int readUnsignedShort() throws IOException {
            return stream.readUnsignedShort();
        }

        @Override
        public char readChar() throws IOException {
            return stream.readChar();
        }

        @Override
        public int readInt() throws IOException {
            return stream.readInt();
        }

        @Override
        public long readLong() throws IOException {
            return stream.readLong();
        }

        @Override
        public float readFloat() throws IOException {
            return stream.readFloat();
        }

        @Override
        public double readDouble() throws IOException {
            return stream.readDouble();
        }

        @Override
        @Deprecated
        public String readLine() throws IOException {
            return stream.readLine();
        }

        @Override
        public String readUTF() throws IOException {
            return stream.readUTF();
        }

    }

    private int findBlockEndOffset(byte[] data, int startOffset) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        // Skip to where the block header should start
        dis.skipBytes(startOffset);
        LOG.info("Reading header at " + startOffset);
        LogBlockHeader header = new LogBlockHeader();
        header.readFields(dis);
        int payloadSize = header.getCompressedDataSize();
        int endOffset = startOffset + header.getSerializedHeaderLength() + payloadSize
            + Bytes.SIZEOF_LONG;
        LOG.info("Block ending offset is " + endOffset);
        return endOffset;
    }

    private List<LogFile.Record> readRecords(LogFileFormatReader reader) throws IOException {
        List<LogFile.Record> decoded = new ArrayList<>();
        LogFile.Record r;
        while ((r = reader.next()) != null) {
            decoded.add(r);
        }
        return decoded;
    }

    private List<LogFile.Record> writeBlock(LogFileFormatWriter writer, String table,
            long startCommitId, int numRecords) throws IOException {
        List<LogFile.Record> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            LogFile.Record r = LogFileTestUtil.newPutRecord(table, startCommitId + i,
                "row" + (startCommitId + i), 10L + i, 1);
            records.add(r);
            writer.append(r);
        }
        writer.closeBlock();
        LOG.info("Next block start position is " + writer.getPosition());
        writer.startBlock();
        return records;
    }

    private void initLogFileReader(byte[] data) throws IOException {
        readerContext.setFileSize(data.length);
        reader.init(readerContext, new PositionedByteArrayInputStream(data));
    }

    private void initLogFileWriter() throws IOException {
        HDFSDataOutput output = new HDFSDataOutput(new FSDataOutputStream(writerDos, null) {
            @Override
            public long getPos() {
                return writerDos.size();
            }
            @Override
            public void hflush() throws IOException {
                writerDos.flush();
            }
            @Override
            public void hsync() throws IOException {
                writerDos.flush();
            }
        });
        writer.init(writerContext, output);
    }

}
