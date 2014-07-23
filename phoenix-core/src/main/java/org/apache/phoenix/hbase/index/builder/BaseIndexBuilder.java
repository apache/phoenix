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
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;

/**
 * Basic implementation of the {@link IndexBuilder} that doesn't do any actual work of indexing.
 * <p>
 * You should extend this class, rather than implementing IndexBuilder directly to maintain
 * compatability going forward.
 * <p>
 * Generally, you should consider using one of the implemented IndexBuilders (e.g
 * {@link CoveredColumnsIndexBuilder}) as there is a lot of work required to keep an index table
 * up-to-date.
 */
public abstract class BaseIndexBuilder implements IndexBuilder {

  private static final Log LOG = LogFactory.getLog(BaseIndexBuilder.class);
  protected boolean stopped;

  @Override
  public void extendBaseIndexBuilderInstead() { }
  
  @Override
  public void setup(RegionCoprocessorEnvironment conf) throws IOException {
    // noop
  }

  @Override
  public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    // noop
  }

  @Override
  public void batchCompleted(MiniBatchOperationInProgress<Mutation> miniBatchOp) {
    // noop
  }
  
  /**
   * By default, we always attempt to index the mutation. Commonly this can be slow (because the
   * framework spends the time to do the indexing, only to realize that you don't need it) or not
   * ideal (if you want to turn on/off indexing on a table without completely reloading it).
 * @throws IOException 
   */
  @Override
  public boolean isEnabled(Mutation m) throws IOException {
    return true; 
  }

  /**
   * {@inheritDoc}
   * <p>
   * By default, assumes that all mutations should <b>not be batched</b>. That is to say, each
   * mutation always applies to different rows, even if they are in the same batch, or are
   * independent updates.
   */
  @Override
  public byte[] getBatchId(Mutation m) {
    return null;
  }

  @Override
  public void stop(String why) {
    LOG.debug("Stopping because: " + why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}