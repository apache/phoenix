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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.hbase.index.StubAbortable;
import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class TestIndexWriter {
  private static final Log LOG = LogFactory.getLog(TestIndexWriter.class);
  @Rule
  public TableName testName = new TableName();
  private final byte[] row = Bytes.toBytes("row");

  @Test
  public void getDefaultWriter() throws Exception {
    Configuration conf = new Configuration(false);
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Mockito.when(env.getConfiguration()).thenReturn(conf);
    assertNotNull(IndexWriter.getCommitter(env));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getDefaultFailurePolicy() throws Exception {
    Configuration conf = new Configuration(false);
    RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
    Region region = Mockito.mock(Region.class);
    Mockito.when(env.getRegion()).thenReturn(region);
    Mockito.when(env.getConfiguration()).thenReturn(conf);
    Mockito.when(region.getTableDesc()).thenReturn(new HTableDescriptor());
    assertNotNull(IndexWriter.getFailurePolicy(env));
  }

  /**
   * With the move to using a pool of threads to write, we need to ensure that we still block until
   * all index writes for a mutation/batch are completed.
   * @throws Exception on failure
   */
  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test
  public void testSynchronouslyCompletesAllWrites() throws Exception {
    LOG.info("Starting " + testName.getTableNameString());
    LOG.info("Current thread is interrupted: " + Thread.interrupted());
    Abortable abort = new StubAbortable();
    Stoppable stop = Mockito.mock(Stoppable.class);
    RegionCoprocessorEnvironment e =Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf =new Configuration();
    Mockito.when(e.getConfiguration()).thenReturn(conf);
    Mockito.when(e.getSharedData()).thenReturn(new ConcurrentHashMap<String,Object>());
    Region mockRegion = Mockito.mock(Region.class);
    Mockito.when(e.getRegion()).thenReturn(mockRegion);
    HTableDescriptor mockTableDesc = Mockito.mock(HTableDescriptor.class);
    Mockito.when(mockRegion.getTableDesc()).thenReturn(mockTableDesc);
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables = new HashMap<ImmutableBytesPtr, HTableInterface>();
    FakeTableFactory factory = new FakeTableFactory(tables);

    byte[] tableName = this.testName.getTableName();
    Put m = new Put(row);
    m.add(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    Collection<Pair<Mutation, byte[]>> indexUpdates = Arrays.asList(new Pair<Mutation, byte[]>(m,
        tableName));

    HTableInterface table = Mockito.mock(HTableInterface.class);
    final boolean[] completed = new boolean[] { false };
    Mockito.when(table.batch(Mockito.anyList())).thenAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        // just keep track that it was called
        completed[0] = true;
        return null;
      }
    });
    Mockito.when(table.getTableName()).thenReturn(testName.getTableName());
    // add the table to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);

    // setup the writer and failure policy
    TrackingParallelWriterIndexCommitter committer = new TrackingParallelWriterIndexCommitter(VersionInfo.getVersion());
    committer.setup(factory, exec, abort, stop, e);
    KillServerOnFailurePolicy policy = new KillServerOnFailurePolicy();
    policy.setup(stop, abort);
    IndexWriter writer = new IndexWriter(committer, policy);
    writer.write(indexUpdates);
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
  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test
  public void testShutdownInterruptsAsExpected() throws Exception {
    Stoppable stop = Mockito.mock(Stoppable.class);
    Abortable abort = new StubAbortable();
    // single thread factory so the older request gets queued
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables = new HashMap<ImmutableBytesPtr, HTableInterface>();
    RegionCoprocessorEnvironment e =Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf =new Configuration();
    Mockito.when(e.getConfiguration()).thenReturn(conf);
    Mockito.when(e.getSharedData()).thenReturn(new ConcurrentHashMap<String,Object>());
    Region mockRegion = Mockito.mock(Region.class);
    Mockito.when(e.getRegion()).thenReturn(mockRegion);
    HTableDescriptor mockTableDesc = Mockito.mock(HTableDescriptor.class);
    Mockito.when(mockRegion.getTableDesc()).thenReturn(mockTableDesc);
    FakeTableFactory factory = new FakeTableFactory(tables);

    byte[] tableName = this.testName.getTableName();
    HTableInterface table = Mockito.mock(HTableInterface.class);
    Mockito.when(table.getTableName()).thenReturn(tableName);
    final CountDownLatch writeStartedLatch = new CountDownLatch(1);
    // latch never gets counted down, so we wait forever
    final CountDownLatch waitOnAbortedLatch = new CountDownLatch(1);
    Mockito.when(table.batch(Mockito.anyList())).thenAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        LOG.info("Write started");
        writeStartedLatch.countDown();
        // when we interrupt the thread for shutdown, we should see this throw an interrupt too
        try {
        waitOnAbortedLatch.await();
        } catch (InterruptedException e) {
          LOG.info("Correctly interrupted while writing!");
          throw e;
        }
        return null;
      }
    });
    // add the tables to the set of tables, so its returned to the writer
    tables.put(new ImmutableBytesPtr(tableName), table);

    // update a single table
    Put m = new Put(row);
    m.add(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    final List<Pair<Mutation, byte[]>> indexUpdates = new ArrayList<Pair<Mutation, byte[]>>();
    indexUpdates.add(new Pair<Mutation, byte[]>(m, tableName));

    // setup the writer
    TrackingParallelWriterIndexCommitter committer = new TrackingParallelWriterIndexCommitter(VersionInfo.getVersion());
    committer.setup(factory, exec, abort, stop, e );
    KillServerOnFailurePolicy policy = new KillServerOnFailurePolicy();
    policy.setup(stop, abort);
    final IndexWriter writer = new IndexWriter(committer, policy);

    final boolean[] failedWrite = new boolean[] { false };
    Thread primaryWriter = new Thread() {

      @Override
      public void run() {
        try {
          writer.write(indexUpdates);
        } catch (IndexWriteException e) {
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