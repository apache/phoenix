/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.write;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.hbase.index.IndexTableName;
import org.apache.phoenix.hbase.index.StubAbortable;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIndexWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestIndexWriter.class);
  @Rule
  public IndexTableName testName = new IndexTableName();
  private final byte[] row = Bytes.toBytes("row");

  @Test
  public void getDefaultWriter() throws Exception {
    Configuration conf = new Configuration(false);
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Mockito.when(env.getConfiguration()).thenReturn(conf);
    assertNotNull(IndexWriter.getCommitter(env));
  }

  @Test
  public void getDefaultFailurePolicy() throws Exception {
    Configuration conf = new Configuration(false);
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Region region = Mockito.mock(Region.class);
    Mockito.when(env.getRegion()).thenReturn(region);
    Mockito.when(env.getConfiguration()).thenReturn(conf);
    Mockito.when(region.getTableDescriptor()).thenReturn(
        TableDescriptorBuilder.newBuilder(TableName.valueOf("dummy")).build());
    assertNotNull(IndexWriter.getFailurePolicy(env));
  }

  /**
   * With the move to using a pool of threads to write, we need to ensure that we still block until
   * all index writes for a mutation/batch are completed.
   * @throws Exception on failure
   */
  @Test
  public void testSynchronouslyCompletesAllWrites() throws Exception {
    LOGGER.info("Starting " + testName.getTableNameString());
    LOGGER.info("Current thread is interrupted: " + Thread.interrupted());
    Abortable abort = new StubAbortable();
    Stoppable stop = Mockito.mock(Stoppable.class);
    RegionCoprocessorEnvironment e =Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf =new Configuration();
    Mockito.when(e.getConfiguration()).thenReturn(conf);
    Mockito.when(e.getSharedData()).thenReturn(new ConcurrentHashMap<String,Object>());
    Region mockRegion = Mockito.mock(Region.class);
    Mockito.when(e.getRegion()).thenReturn(mockRegion);
    TableDescriptor mockTableDesc = Mockito.mock(TableDescriptor.class);
    Mockito.when(mockRegion.getTableDescriptor()).thenReturn(mockTableDesc);
    TableName mockTN = TableName.valueOf("test");
    Mockito.when(mockTableDesc.getTableName()).thenReturn(mockTN);
    Connection mockConnection = Mockito.mock(Connection.class);
    Mockito.when(e.getConnection()).thenReturn(mockConnection);
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, Table> tables = new HashMap<ImmutableBytesPtr, Table>();
    FakeTableFactory factory = new FakeTableFactory(tables);

    byte[] tableName = this.testName.getTableName();
    Put m = new Put(row);
    m.addColumn(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    Collection<Pair<Mutation, byte[]>> indexUpdates = Arrays.asList(new Pair<Mutation, byte[]>(m,
        tableName));

    Table table = Mockito.mock(Table.class);
    final boolean[] completed = new boolean[] { false };
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                // just keep track that it was called
                completed[0] = true;
                return null;
            }
        }).when(table).batch(Mockito.anyList(), Mockito.any());
    Mockito.when(table.getName()).thenReturn(TableName.valueOf(testName.getTableName()));
    // add the table to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);

    // setup the writer and failure policy
    TrackingParallelWriterIndexCommitter committer = new TrackingParallelWriterIndexCommitter(VersionInfo.getVersion());
    committer.setup(factory, exec, stop, e);
    KillServerOnFailurePolicy policy = new KillServerOnFailurePolicy();
    policy.setup(stop, e);
    IndexWriter writer = new IndexWriter(committer, policy);
    writer.write(indexUpdates, ScanUtil.UNKNOWN_CLIENT_VERSION);
    assertTrue("Writer returned before the table batch completed! Likely a race condition tripped",
      completed[0]);
    writer.stop(this.testName.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
  }

  /**
   * Test that if we get an interruption to to the thread while doing a batch (e.g. via shutdown),
   * that we correctly end the task
   * @throws Exception on failure
   */
  @Test
  public void testShutdownInterruptsAsExpected() throws Exception {
    Stoppable stop = Mockito.mock(Stoppable.class);
    Abortable abort = new StubAbortable();
    // single thread factory so the older request gets queued
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, Table> tables = new HashMap<ImmutableBytesPtr, Table>();
    RegionCoprocessorEnvironment e =Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf =new Configuration();
    Mockito.when(e.getConfiguration()).thenReturn(conf);
    Mockito.when(e.getSharedData()).thenReturn(new ConcurrentHashMap<String,Object>());
    Region mockRegion = Mockito.mock(Region.class);
    Mockito.when(e.getRegion()).thenReturn(mockRegion);
    TableDescriptor mockTableDesc = Mockito.mock(TableDescriptor.class);
    Mockito.when(mockRegion.getTableDescriptor()).thenReturn(mockTableDesc);
    Mockito.when(mockTableDesc.getTableName()).thenReturn(TableName.valueOf("test"));
    Connection mockConnection = Mockito.mock(Connection.class);
    Mockito.when(e.getConnection()).thenReturn(mockConnection);
    FakeTableFactory factory = new FakeTableFactory(tables);

    byte[] tableName = this.testName.getTableName();
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.getName()).thenReturn(TableName.valueOf(tableName));
    final CountDownLatch writeStartedLatch = new CountDownLatch(1);
    // latch never gets counted down, so we wait forever
    final CountDownLatch waitOnAbortedLatch = new CountDownLatch(1);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                LOGGER.info("Write started");
                writeStartedLatch.countDown();
                // when we interrupt the thread for shutdown, we should see this throw an interrupt too
                try {
                    waitOnAbortedLatch.await();
                } catch (InterruptedException e) {
                    LOGGER.info("Correctly interrupted while writing!");
                    throw e;
                }
                return null;
            }
        }).when(table).batch(Mockito.anyListOf(Row.class), Mockito.any());
    // add the tables to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);

    // update a single table
    Put m = new Put(row);
    m.addColumn(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    final List<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>();
    indexUpdates.add(new Pair<Mutation, byte[]>(m, tableName));

    // setup the writer
    TrackingParallelWriterIndexCommitter committer = new TrackingParallelWriterIndexCommitter(VersionInfo.getVersion());
    committer.setup(factory, exec, stop, e );
    KillServerOnFailurePolicy policy = new KillServerOnFailurePolicy();
    policy.setup(stop, e);
    final IndexWriter writer = new IndexWriter(committer, policy);

    final boolean[] failedWrite = new boolean[] { false };
    Thread primaryWriter = new Thread() {

      @Override
      public void run() {
        try {
          writer.write(indexUpdates, ScanUtil.UNKNOWN_CLIENT_VERSION);
        } catch (IndexWriteException e) {
          failedWrite[0] = true;
        } catch (IOException e) {
          failedWrite[0] = true;
        }
      }
    };
    primaryWriter.start();
    // wait for the write to start before intentionally shutdown the pool
    writeStartedLatch.await();
    writer.stop("Shutting down writer for test " + this.testName.getTableNameString());
    primaryWriter.join();
    assertTrue("Writer should have failed because of the stop we issued", failedWrite[0]);
  }
}