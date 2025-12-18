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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class LogFileWriterSyncTest {

  private Configuration conf;
  private FileSystem mockFs;
  private SyncableDataOutput internalOutput;
  private LogFileWriterContext writerContext;
  private LogFileWriter writer;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();

    // This is structured so we verify that FSDataOutputStream correctly calls hsync() on its
    // understream, which in this case will be our SyncableByteArrayOutputStream.
    // The sync flow is: LogFileWriter -sync-> LogFileFormatWriter -sync-> HDFSDataOutput
    // -hsync-> FSDataOutputStream -hsync-> internalOutput
    internalOutput = spy(new LogFileTestUtil.SyncableByteArrayOutputStream());
    mockFs = mock(FileSystem.class);
    when(mockFs.create(any(), anyBoolean())).thenReturn(
      new FSDataOutputStream((OutputStream) internalOutput, new FileSystem.Statistics("hdfs"), 0));
    when(internalOutput.getPos()).thenReturn(100L);

    // Create context using mocks
    writerContext = new LogFileWriterContext(conf).setFileSystem(mockFs);

    // Create the writer instance to be tested
    writer = new LogFileWriter();

    // Initialize the writer - this will call fs.create() and set up internal writers
    writer.init(writerContext);
  }

  @After
  public void tearDown() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  @Test
  public void testSyncAfterAppend() throws IOException {
    InOrder inOrder = inOrder(internalOutput);

    // Append data
    Mutation m1 = LogFileTestUtil.newPut("row1", 1L, 1);
    writer.append("TBL", 1L, m1);
    Mutation m2 = LogFileTestUtil.newPut("row2", 2L, 1);
    writer.append("TBL", 2L, m2);

    // Sync the writer
    writer.sync();

    // Verify hsync was called *after* the data related to appends would have been written
    // We know sync() forces a block close and hsync on the underlying stream.
    inOrder.verify(internalOutput, times(1)).hsync();

    // Append more data after sync
    Mutation m3 = LogFileTestUtil.newPut("row3", 12L, 1);
    writer.append("TBL", 3L, m3);

    // Sync again
    writer.sync();

    // Verify hsync was called again
    inOrder.verify(internalOutput, times(1)).hsync();
  }

  @Test
  public void testAppendAfterSync() throws IOException {
    InOrder inOrder = inOrder(internalOutput);

    // Append A, then sync
    Mutation m1 = LogFileTestUtil.newPut("row1", 1L, 1);
    writer.append("TBL", 1L, m1);
    writer.sync();

    // Verify first hsync
    inOrder.verify(internalOutput, times(1)).hsync();

    // Append B after sync
    Mutation m2 = LogFileTestUtil.newPut("row2", 2L, 1);
    writer.append("TBL", 2L, m2);

    // Verify hsync was NOT called immediately after appending B. It might be called later on
    // close or another sync.
    inOrder.verify(internalOutput, never()).hsync();

    // Now close (which includes a sync)
    writer.close();

    // Verify the final hsync happened during close
    inOrder.verify(internalOutput, times(1)).hsync();
  }

  @Test
  public void testMultipleSyncs() throws IOException {
    InOrder inOrder = inOrder(internalOutput);

    // Append A
    Mutation m1 = LogFileTestUtil.newPut("row1", 1L, 1);
    writer.append("TBL", 1L, m1);

    // Sync 1
    writer.sync();
    inOrder.verify(internalOutput, times(1)).hsync();

    // Sync 2 (no data appended in between)
    writer.sync();
    // The current implementation should do nothing if the block is empty
    inOrder.verify(internalOutput, never()).hsync();

    // Append B
    Mutation m2 = LogFileTestUtil.newPut("row2", 2L, 1);
    writer.append("TBL", 2L, m2);

    // Sync 3
    writer.sync();
    inOrder.verify(internalOutput, times(1)).hsync();

    // Now close (which includes a sync)
    writer.close();

    // A total of three hsyncs
    verify(internalOutput, times(3)).hsync();
  }

  @Test
  public void testSyncEmpty() throws IOException {
    InOrder inOrder = inOrder(internalOutput);

    // Sync immediately after init
    writer.sync();

    // The current implementation should do nothing if the block is empty
    inOrder.verify(internalOutput, never()).hsync();

    // Append a record
    Mutation m1 = LogFileTestUtil.newPut("row", 1L, 1);
    writer.append("TBL", 1L, m1);

    // Sync again
    writer.sync();

    // Verify the second hsync
    inOrder.verify(internalOutput, times(1)).hsync();

    // A total of one hsync
    verify(internalOutput, times(1)).hsync();
  }

}
