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
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileWriterTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogFileWriterTest.class);

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private Path filePath;
    private LogFileReaderContext readerContext;
    private LogFileReader reader;
    private LogFileWriterContext writerContext;
    private LogFileWriter writer;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        filePath = new Path(testFolder.newFile("LogFileWriterTest").toURI());
        reader = new LogFileReader();
        writer = new LogFileWriter();
    }

    @After
    public void tearDown() throws IOException {
        writer.close();
        reader.close();
    }

    @Test
    public void testLogFileWriter() throws IOException {
        initLogFileWriter();
        LogFile.Record r1 = LogFileTestUtil.newPutRecord("TBLTLFW", 100, "row1", 10L, 1);
        LogFile.Record r2 = LogFileTestUtil.newDeleteRecord("TBLTLFW", 101, "row2", 11L, 1);
        writer.append(r1.getHBaseTableName(), r1.getCommitId(), r1.getMutation());
        LOG.debug("Appended " + r1);
        writer.sync();
        writer.append(r2.getHBaseTableName(), r2.getCommitId(), r2.getMutation());
        LOG.debug("Appended " + r2);
        writer.close();

        assertTrue("File should exist", localFs.exists(filePath));
        assertTrue("File length should be > 0", writer.getLength() > 0);

        initLogFileReader();
        assertEquals("Header major version mismatch", LogFileHeader.VERSION_MAJOR,
            reader.getHeader().getMajorVersion());

        LogFile.Record decoded1 = reader.next();
        LOG.debug("Read " + decoded1);
        LogFileTestUtil.assertRecordEquals("First record mismatch", r1, decoded1);

        LogFile.Record decoded2 = reader.next();
        LOG.debug("Read " + decoded2);
        LogFileTestUtil.assertRecordEquals("Second record mismatch", r2, decoded2);

        assertNull("Should be end of file", reader.next());
        assertNotNull("Trailer should exist", reader.getTrailer());
        reader.close();
    }

    @Test
    public void testLogFileReaderIterator() throws IOException {
        initLogFileWriter();
        List<LogFile.Record> originals = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            LogFile.Record record = LogFileTestUtil.newPutRecord("TBLFRI", 100L + i, "row" + i,
                10L + i, 1);
            originals.add(record);
            writer.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
        }
        writer.close();

        initLogFileReader();
        List<LogFile.Record> decoded = new ArrayList<>();
        Iterator<LogFile.Record> iterator = reader.iterator();
        while (iterator.hasNext()) {
            LogFile.Record r = iterator.next();
            decoded.add(r);
        }

        assertEquals("Iterator count mismatch", originals.size(), decoded.size());
        for (int i = 0; i < originals.size(); i++) {
            LogFileTestUtil.assertRecordEquals("Iterator record " + i + " mismatch",
                originals.get(i), decoded.get(i));
        }
        reader.close();
    }

    @Test
    public void testLogFileReaderEmptyFile() throws IOException {
        initLogFileWriter();
        writer.close(); // Creates an empty file (header + trailer)
        initLogFileReader();
        assertNull("Next should return null for empty file", reader.next());
        reader.close();
    }

    @Test
    public void testHeaderWrittenImmediately() throws IOException {
        // This should write header immediately
        initLogFileWriter();
        // Verify file exists and has content (header should be written)
        assertTrue("File should exist after init", localFs.exists(filePath));
        assertEquals("File should have header written", LogFileHeader.HEADERSIZE, writer.getLength());
        writer.close();
    }

    private void initLogFileReader() throws IOException {
        readerContext = new LogFileReaderContext(conf).setFileSystem(localFs)
            .setFilePath(filePath);
        reader.init(readerContext);
    }

    private void initLogFileWriter() throws IOException {
        writerContext = new LogFileWriterContext(conf).setFileSystem(localFs)
            .setFilePath(filePath);
        writer.init(writerContext);
    }

}
