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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.common.collect.Multimap;

import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;

/**
 * Naive failure policy - kills the server on which it resides
 */
public class KillServerOnFailurePolicy implements IndexFailurePolicy {

  private static final Log LOG = LogFactory.getLog(KillServerOnFailurePolicy.class);
  private Abortable abortable;
  private Stoppable stoppable;

  @Override
  public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
    setup(parent, env.getRegionServerServices());
  }

  public void setup(Stoppable parent, Abortable abort) {
    this.stoppable = parent;
    this.abortable = abort;
  }

  @Override
  public void stop(String why) {
    // noop
  }

  @Override
  public boolean isStopped() {
    return this.stoppable.isStopped();
  }

  @Override
  public void
      handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause){
    // cleanup resources
    this.stop("Killing ourselves because of an error:" + cause);
    // notify the regionserver of the failure
    String msg =
        "Could not update the index table, killing server region because couldn't write to an index table";
    LOG.error(msg, cause);
    try {
      this.abortable.abort(msg, cause);
    } catch (Exception e) {
      LOG.fatal("Couldn't abort this server to preserve index writes, "
          + "attempting to hard kill the server");
      System.exit(1);
    }

  }

}
