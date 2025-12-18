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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class LogFileCompressionTest {

  private static final Logger LOG = LoggerFactory.getLogger(LogFileCompressionTest.class);

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  private static Configuration conf;
  private FileSystem localFs;
  private Path filePath;
  private Compression.Algorithm compression;
  private LogFileReaderContext readerContext;
  private LogFileReader reader;
  private LogFileWriterContext writerContext;
  private LogFileWriter writer;

  public LogFileCompressionTest(Compression.Algorithm compression) {
    this.compression = compression;
  }

  @Parameters(name = "Compression={0}")
  public static Collection<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    // NONE
    params.add(new Object[] { Compression.Algorithm.NONE });
    // LZ4, LZO, SNAPPY, and ZSTD provided by hbase-compression-aircompressor
    params.add(new Object[] { Compression.Algorithm.LZ4 });
    params.add(new Object[] { Compression.Algorithm.LZO });
    params.add(new Object[] { Compression.Algorithm.SNAPPY });
    params.add(new Object[] { Compression.Algorithm.ZSTD });
    // GZ provided by the JRE
    params.add(new Object[] { Compression.Algorithm.GZ });
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    conf = HBaseConfiguration.create();
    // Use the pure java AirCompressor codecs so we don't need to worry about environment
    // issues.
    conf.set("hbase.io.compress.lz4.codec",
      "org.apache.hadoop.hbase.io.compress.aircompressor.Lz4Codec");
    Compression.Algorithm.LZ4.reload(conf);
    conf.set("hbase.io.compress.lzo.codec",
      "org.apache.hadoop.hbase.io.compress.aircompressor.LzoCodec");
    Compression.Algorithm.LZO.reload(conf);
    conf.set("hbase.io.compress.snappy.codec",
      "org.apache.hadoop.hbase.io.compress.aircompressor.SnappyCodec");
    Compression.Algorithm.SNAPPY.reload(conf);
    conf.set("hbase.io.compress.zstd.codec",
      "org.apache.hadoop.hbase.io.compress.aircompressor.ZstdCodec");
    Compression.Algorithm.ZSTD.reload(conf);
  }

  @Before
  public void setUp() throws IOException {
    localFs = FileSystem.getLocal(conf);
    // Use a unique path for each test instance based on compression
    filePath = new Path(
      testFolder.newFile("LogCompressionTest_" + testName.getMethodName()).getAbsolutePath());
    reader = new LogFileReader();
    writer = new LogFileWriter();
  }

  @After
  public void tearDown() throws IOException {
    if (writer != null) {
      writer.close();
    }
    if (reader != null) {
      reader.close();
    }
  }

  @Test
  public void testLogFileSingleBlockWithCompression() throws IOException {
    LOG.info("Testing single block with compression {}", compression.getName());
    initLogFileWriter(LogFileWriterContext.DEFAULT_LOGFILE_BLOCK_SIZE);
    List<LogFile.Record> originals = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      LogFile.Record record = LogFileTestUtil.newPutRecord("TBLSBWC", i, "row" + i, 100L + i, 2);
      originals.add(record);
      writer.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
    }
    writer.close();
    initLogFileReader();
    readAndVerifyRecords(originals, 1); // Expect exactly one block
  }

  @Test
  public void testLogFileMultipleBlocksWithCompression() throws IOException {
    LOG.info("Testing multiple blocks with compression {}", compression.getName());
    // Use a small block size to force multiple blocks
    initLogFileWriter(8 * 1024); // 8k
    List<LogFile.Record> originals = new ArrayList<>();
    // Generate enough records to span multiple blocks
    for (int i = 0; i < 100_000; i++) {
      LogFile.Record record = LogFileTestUtil.newPutRecord("TBLMBWC", i, "row" + i, 100L + i, 5);
      originals.add(record);
      writer.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
    }
    writer.close();
    initLogFileReader();
    assertTrue("File has more than one block", reader.getTrailer().getBlockCount() > 1);
    readAndVerifyRecords(originals, reader.getTrailer().getBlockCount());
  }

  @Test
  public void testLogFileEmptyFile() throws IOException {
    LOG.info("Testing empty file with compression {}", compression.getName());
    initLogFileWriter(LogFileWriterContext.DEFAULT_LOGFILE_BLOCK_SIZE);
    writer.close(); // Creates an empty file (header + trailer)
    initLogFileReader();
    assertNull("Next should return null for empty file", reader.next());
    reader.close();
  }

  private void readAndVerifyRecords(List<LogFile.Record> originals, long expectedBlockCount)
    throws IOException {
    assertTrue("Test file does not exist: " + filePath, localFs.exists(filePath));
    assertTrue("Test file has zero length: " + filePath,
      localFs.getFileStatus(filePath).getLen() > 0);

    // Verify Header
    LogFile.Header header = reader.getHeader();
    assertNotNull("Header should not be null", header);

    // Read records using iterator
    List<LogFile.Record> decoded = new ArrayList<>();
    Iterator<LogFile.Record> iterator = reader.iterator();
    while (iterator.hasNext()) {
      decoded.add(iterator.next());
    }

    // Verify Records
    assertEquals("Number of decoded records mismatch", originals.size(), decoded.size());
    for (int i = 0; i < originals.size(); i++) {
      LogFileTestUtil.assertRecordEquals("Record " + i + " mismatch", originals.get(i),
        decoded.get(i));
    }

    // Verify Trailer
    LogFile.Trailer trailer = reader.getTrailer();
    assertNotNull("Trailer should not be null", trailer);
    assertEquals("Trailer record count mismatch", originals.size(), trailer.getRecordCount());
    assertEquals("Trailer block count mismatch", expectedBlockCount, trailer.getBlockCount());

    // Verify Reader Context Counters
    assertEquals("Reader context record count mismatch", trailer.getRecordCount(),
      readerContext.getRecordsRead());
    assertEquals("Reader context block count mismatch", trailer.getBlockCount(),
      readerContext.getBlocksRead());
    assertEquals("Reader context corrupt blocks skipped count should be 0", 0,
      readerContext.getCorruptBlocksSkipped());
  }

  private void initLogFileReader() throws IOException {
    readerContext = new LogFileReaderContext(conf).setFileSystem(localFs).setFilePath(filePath);
    reader.init(readerContext);
  }

  private void initLogFileWriter(long maxBlockSize) throws IOException {
    writerContext = new LogFileWriterContext(conf).setCompression(compression)
      .setFileSystem(localFs).setFilePath(filePath).setMaxBlockSize(maxBlockSize);
    writer.init(writerContext);
  }

}
