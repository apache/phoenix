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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class LogCompressionTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogCompressionTest.class);

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private Path filePath;
    private Compression.Algorithm compression;
    private LogReaderContext readerContext;
    private LogReader reader;
    private LogWriterContext writerContext;
    private LogWriter writer;

    public LogCompressionTest(Compression.Algorithm compression) {
        this.compression = compression;
    }

    @Parameters(name = "Compression={0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{Compression.Algorithm.NONE});
        params.add(new Object[]{Compression.Algorithm.LZ4});
        params.add(new Object[]{Compression.Algorithm.SNAPPY});
        params.add(new Object[]{Compression.Algorithm.ZSTD});
        return params;
    }

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        // Use the pure java AirCompressor codecs so we don't need to worry about environment
        // issues.
        conf.set("hbase.io.compress.lz4.codec",
            "org.apache.hadoop.hbase.io.compress.aircompressor.Lz4Codec");
        conf.set("hbase.io.compress.snappy.codec",
            "org.apache.hadoop.hbase.io.compress.aircompressor.SnappyCodec");
        conf.set("hbase.io.compress.zstd.codec",
            "org.apache.hadoop.hbase.io.compress.aircompressor.ZstdCodec");
        localFs = FileSystem.getLocal(conf);
        // Use a unique path for each test instance based on compression
        filePath = new Path(testFolder.newFile("LogCompressionTest_"
            + compression.getName()).toURI());
        reader = new LogReader();
        writer = new LogWriter();
    }

    @After
    public void tearDown() throws IOException {
        writer.close();
        reader.close();
    }

    @Test
    public void testSingleBlockWithCompression() throws IOException {
        LOG.info("Testing single block with compression {}", compression.getName());
        initLogWriter(LogWriterContext.DEFAULT_LOG_BLOCK_SIZE);
        List<Log.Record> originals = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Log.Record r = newRecord("TBL1", (long)i, "row" + i, 100L + i, 2);
            originals.add(r);
            writer.append(r);
        }
        writer.close();
        initLogReader();
        readAndVerifyRecords(originals, 1); // Expect exactly one block
    }

    @Test
    public void testMultipleBlocksWithCompression() throws IOException {
        LOG.info("Testing multiple blocks with compression {}", compression.getName());
        // Use a small block size to force multiple blocks
        initLogWriter(8 * 1024); // 8k
        List<Log.Record> originals = new ArrayList<>();
        // Generate enough records to span multiple blocks
        for (int i = 0; i < 100_000; i++) {
            Log.Record r = newRecord("TBL_MULTI", (long)i, "row" + i, 200L + i, 5);
            originals.add(r);
            writer.append(r);
        }
        writer.close();
        initLogReader();
        assertTrue("File has more than one block", reader.getTrailer().getBlockCount() > 1);
        readAndVerifyRecords(originals, reader.getTrailer().getBlockCount());
    }

    @Test
    public void testEmptyFile() throws IOException {
        LOG.info("Testing empty file with compression {}", compression.getName());
        initLogWriter(LogWriterContext.DEFAULT_LOG_BLOCK_SIZE);
        writer.close(); // Creates an empty file (header + trailer)
        initLogReader();
        assertNull("Next should return null for empty file", reader.next());
        reader.close();
    }

    private void initLogReader() throws IOException {
        readerContext = new LogReaderContext(conf)
            .setFileSystem(localFs)
            .setFilePath(filePath);
        reader.init(readerContext);
    }

    private void initLogWriter(long maxBlockSize) throws IOException {
        writerContext = new LogWriterContext(conf)
            .setCompression(compression)
            .setFileSystem(localFs)
            .setFilePath(filePath)
            .setMaxBlockSize(maxBlockSize);
        writer.init(writerContext);
    }

    private Log.Record newRecord(String table, long commitId, String rowKey, long ts,
            int numCols) {
        Log.Record record = new LogRecord()
            .setMutationType(Log.MutationType.PUT)
            .setSchemaObjectName(table)
            .setCommitId(commitId)
            .setRowKey(Bytes.toBytes(rowKey))
            .setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            record.addColumnValue(Bytes.toBytes("col" + i), Bytes.toBytes("v" + i + "_" + rowKey));
        }
        return record;
    }

    private void readAndVerifyRecords(List<Log.Record> originalRecords, long expectedBlockCount)
            throws IOException {
        assertTrue("Test file does not exist: " + filePath, localFs.exists(filePath));
        assertTrue("Test file has zero length: " + filePath,
            localFs.getFileStatus(filePath).getLen() > 0);

        // Verify Header
        Log.Header header = reader.getHeader();
        assertNotNull("Header should not be null", header);

        // Read records using iterator
        List<Log.Record> decodedRecords = new ArrayList<>();
        Iterator<Log.Record> iterator = reader.iterator();
        while (iterator.hasNext()) {
          decodedRecords.add(iterator.next());
        }

        // Verify Records
        assertEquals("Number of decoded records mismatch", originalRecords.size(),
            decodedRecords.size());
        for (int i = 0; i < originalRecords.size(); i++) {
          assertEquals("Record " + i + " mismatch", originalRecords.get(i), decodedRecords.get(i));
        }

        // Verify Trailer
        Log.Trailer trailer = reader.getTrailer();
        assertNotNull("Trailer should not be null", trailer);
        assertEquals("Trailer record count mismatch", originalRecords.size(),
            trailer.getRecordCount());
        assertEquals("Trailer block count mismatch", expectedBlockCount,
            trailer.getBlockCount());

        // Verify Reader Context Counters
        assertEquals("Reader context record count mismatch", trailer.getRecordCount(),
            readerContext.getRecordsRead());
        assertEquals("Reader context block count mismatch", trailer.getBlockCount(),
            readerContext.getBlocksRead());
        assertEquals("Reader context corrupt blocks skipped count should be 0", 0,
            readerContext.getCorruptBlocksSkipped());
    }

}
