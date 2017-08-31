/**
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
package org.apache.phoenix.hbase.index.write.recovery;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.google.common.collect.Multimap;
import org.apache.phoenix.hbase.index.exception.MultiIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.IndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.KillServerOnFailurePolicy;

/**
 * Tracks any failed writes in The {@link PerRegionIndexWriteCache}, given a
 * {@link MultiIndexWriteFailureException} (which is thrown from the
 * {@link TrackingParallelWriterIndexCommitter}. Any other exception failure causes the a server
 * abort via the usual {@link KillServerOnFailurePolicy}.
 */
public class StoreFailuresInCachePolicy implements IndexFailurePolicy {

  private KillServerOnFailurePolicy delegate;
  private PerRegionIndexWriteCache cache;
  private HRegion region;

  /**
   * @param failedIndexEdits cache to update when we find a failure
   */
  public StoreFailuresInCachePolicy(PerRegionIndexWriteCache failedIndexEdits) {
    this.cache = failedIndexEdits;
  }

  @Override
  public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
    this.region = env.getRegion();
    this.delegate = new KillServerOnFailurePolicy();
    this.delegate.setup(parent, env);

  }

  @Override
  public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause) throws IOException {
    // if its not an exception we can handle, let the delegate take care of it
    if (!(cause instanceof MultiIndexWriteFailureException)) {
      delegate.handleFailure(attempted, cause);
    }
    List<HTableInterfaceReference> failedTables =
        ((MultiIndexWriteFailureException) cause).getFailedTables();
    for (HTableInterfaceReference table : failedTables) {
      cache.addEdits(this.region, table, attempted.get(table));
    }
  }


  @Override
  public void stop(String why) {
    this.delegate.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.delegate.isStopped();
  }
}