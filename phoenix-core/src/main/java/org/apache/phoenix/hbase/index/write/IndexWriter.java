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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * Do the actual work of writing to the index tables. Ensures that if we do fail to write to the
 * index table that we cleanly kill the region/server to ensure that the region's WAL gets replayed.
 * <p>
 * We attempt to do the index updates in parallel using a backing threadpool. All threads are daemon
 * threads, so it will not block the region from shutting down.
 */
public class IndexWriter implements Stoppable {

  private static final Log LOG = LogFactory.getLog(IndexWriter.class);
  private static final String INDEX_COMMITTER_CONF_KEY = "index.writer.commiter.class";
  public static final String INDEX_FAILURE_POLICY_CONF_KEY = "index.writer.failurepolicy.class";
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private IndexCommitter writer;
  private IndexFailurePolicy failurePolicy;

  /**
   * @throws IOException if the {@link IndexWriter} or {@link IndexFailurePolicy} cannot be
   *           instantiated
   */
  public IndexWriter(RegionCoprocessorEnvironment env, String name) throws IOException {
    this(getCommitter(env), getFailurePolicy(env), env, name);
  }

  public static IndexCommitter getCommitter(RegionCoprocessorEnvironment env) throws IOException {
    Configuration conf = env.getConfiguration();
    try {
      IndexCommitter committer =
          conf.getClass(INDEX_COMMITTER_CONF_KEY, ParallelWriterIndexCommitter.class,
            IndexCommitter.class).newInstance();
      return committer;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  public static IndexFailurePolicy getFailurePolicy(RegionCoprocessorEnvironment env)
      throws IOException {
    Configuration conf = env.getConfiguration();
    try {
      IndexFailurePolicy committer =
          conf.getClass(INDEX_FAILURE_POLICY_CONF_KEY, KillServerOnFailurePolicy.class,
            IndexFailurePolicy.class).newInstance();
      return committer;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  /**
   * Directly specify the {@link IndexCommitter} and {@link IndexFailurePolicy}. Both are expected
   * to be fully setup before calling.
   * @param committer
   * @param policy
   * @param env
   */
  public IndexWriter(IndexCommitter committer, IndexFailurePolicy policy,
      RegionCoprocessorEnvironment env, String name) {
    this(committer, policy);
    this.writer.setup(this, env, name);
    this.failurePolicy.setup(this, env);
  }

  /**
   * Create an {@link IndexWriter} with an already setup {@link IndexCommitter} and
   * {@link IndexFailurePolicy}.
   * @param committer to write updates
   * @param policy to handle failures
   */
  IndexWriter(IndexCommitter committer, IndexFailurePolicy policy) {
    this.writer = committer;
    this.failurePolicy = policy;
  }
  
  /**
   * Write the mutations to their respective table.
   * <p>
   * This method is blocking and could potentially cause the writer to block for a long time as we
   * write the index updates. When we return depends on the specified {@link IndexCommitter}.
   * <p>
   * If update fails, we pass along the failure to the installed {@link IndexFailurePolicy}, which
   * then decides how to handle the failure. By default, we use a {@link KillServerOnFailurePolicy},
   * which ensures that the server crashes when an index write fails, ensuring that we get WAL
   * replay of the index edits.
   * @param indexUpdates Updates to write
 * @throws IOException 
   */
  public void writeAndKillYourselfOnFailure(Collection<Pair<Mutation, byte[]>> indexUpdates) throws IOException  {
    // convert the strings to htableinterfaces to which we can talk and group by TABLE
    Multimap<HTableInterfaceReference, Mutation> toWrite = resolveTableReferences(indexUpdates);
    writeAndKillYourselfOnFailure(toWrite);
  }

  /**
   * see {@link #writeAndKillYourselfOnFailure(Collection)}.
   * @param toWrite
 * @throws IOException 
   */
  public void writeAndKillYourselfOnFailure(Multimap<HTableInterfaceReference, Mutation> toWrite) throws IOException {
    try {
      write(toWrite);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Done writing all index updates!\n\t" + toWrite);
      }
    } catch (Exception e) {
      this.failurePolicy.handleFailure(toWrite, e);
    }
  }

  /**
   * Write the mutations to their respective table.
   * <p>
   * This method is blocking and could potentially cause the writer to block for a long time as we
   * write the index updates. We only return when either:
   * <ol>
   * <li>All index writes have returned, OR</li>
   * <li>Any single index write has failed</li>
   * </ol>
   * We attempt to quickly determine if any write has failed and not write to the remaining indexes
   * to ensure a timely recovery of the failed index writes.
   * @param toWrite Updates to write
   * @throws IndexWriteException if we cannot successfully write to the index. Whether or not we
   *           stop early depends on the {@link IndexCommitter}.
   */
  public void write(Collection<Pair<Mutation, byte[]>> toWrite) throws IndexWriteException {
    write(resolveTableReferences(toWrite));
  }

  /**
   * see {@link #write(Collection)}
   * @param toWrite
   * @throws IndexWriteException
   */
  public void write(Multimap<HTableInterfaceReference, Mutation> toWrite)
      throws IndexWriteException {
    this.writer.write(toWrite);
  }


  /**
   * Convert the passed index updates to {@link HTableInterfaceReference}s.
   * @param indexUpdates from the index builder
   * @return pairs that can then be written by an {@link IndexWriter}.
   */
  public static Multimap<HTableInterfaceReference, Mutation> resolveTableReferences(
      Collection<Pair<Mutation, byte[]>> indexUpdates) {
    Multimap<HTableInterfaceReference, Mutation> updates = ArrayListMultimap
        .<HTableInterfaceReference, Mutation> create();
    // simple map to make lookups easy while we build the map of tables to create
    Map<ImmutableBytesPtr, HTableInterfaceReference> tables =
        new HashMap<ImmutableBytesPtr, HTableInterfaceReference>(updates.size());
    for (Pair<Mutation, byte[]> entry : indexUpdates) {
      byte[] tableName = entry.getSecond();
      ImmutableBytesPtr ptr = new ImmutableBytesPtr(tableName);
      HTableInterfaceReference table = tables.get(ptr);
      if (table == null) {
        table = new HTableInterfaceReference(ptr);
        tables.put(ptr, table);
      }
      updates.put(table, entry.getFirst());
    }

    return updates;
  }

  @Override
  public void stop(String why) {
    if (!this.stopped.compareAndSet(false, true)) {
      // already stopped
      return;
    }
    LOG.debug("Stopping because " + why);
    this.writer.stop(why);
    this.failurePolicy.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.stopped.get();
  }
}