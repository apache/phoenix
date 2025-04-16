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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogWriterTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private Path filePath;
    private LogReader reader;
    private LogWriter writer;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        filePath = new Path(testFolder.newFile("LogWriterTest").toURI());
        reader = new LogReader();
        writer = new LogWriter();
    }

    @After
    public void tearDown() throws IOException {
        writer.close();
        reader.close();
    }

    @Test
    public void testLogWriter() throws IOException {
        initLogWriter();
        Log.Record r1 = newRecord("TBL1", 1L, "row1", 10L, 1);
        Log.Record r2 = newRecord("TBL1", 2L, "row2", 11L, 1);
        writer.append(r1);
        writer.sync();
        writer.append(r2);
        writer.close();

        assertTrue("File should exist", localFs.exists(filePath));
        assertTrue("File length should be > 0", writer.getLength() > 0);

        initLogReader();
        assertEquals("Header major version mismatch", Log.VERSION_MAJOR,
            reader.getHeader().getMajorVersion());

        LogRecord decoded1 = (LogRecord) reader.next();
        assertEquals("First record mismatch", r1, decoded1);

        LogRecord decoded2 = (LogRecord) reader.next();
        assertEquals("Second record mismatch", r2, decoded2);

        assertNull("Should be end of file", reader.next());
        assertNotNull("Trailer should exist", reader.getTrailer());
        reader.close();
    }

    @Test
    public void testLogReaderIterator() throws IOException {
        initLogWriter();
        List<Log.Record> originals = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Log.Record r = newRecord("ITER", (long)i, "row" + i, 20L + i, 1);
            originals.add(r);
            writer.append(r);
        }
        writer.close();

        initLogReader();
        List<Log.Record> decoded = new ArrayList<>();
        Iterator<Log.Record> iterator = reader.iterator();
        while (iterator.hasNext()) {
            decoded.add(iterator.next());
        }

        assertEquals("Iterator count mismatch", originals.size(), decoded.size());
        for (int i = 0; i < originals.size(); i++) {
            assertEquals("Iterator record " + i + " mismatch", originals.get(i), decoded.get(i));
        }
        reader.close();
    }

    @Test
    public void testLogReaderEmptyFile() throws IOException {
        initLogWriter();
        writer.close(); // Creates an empty file (header + trailer)
        initLogReader();
        assertNull("Next should return null for empty file", reader.next());
        reader.close();
    }

    private void initLogReader() throws IOException {
        LogReaderContext ctx = new LogReaderContext(conf)
            .setFileSystem(localFs)
            .setFilePath(filePath);
        reader.init(ctx);
    }

    private void initLogWriter() throws IOException {
        LogWriterContext ctx = new LogWriterContext(conf)
            .setFileSystem(localFs)
            .setFilePath(filePath);
        writer.init(ctx);
    }

    private Log.Record newRecord(String table, long commitId, String rowKey, long ts, int numCols) {
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

}
