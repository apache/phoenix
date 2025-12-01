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
package org.apache.phoenix.replication;

import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC_AND_FORWARD;

import java.io.IOException;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sync and Forward mode implementation
 * <p>
 * This class implements the sync and forward replication mode. It delegates the append and sync
 * events to the replication log on the standby cluster. In the background, it also forwards the
 * replication log from the fallback cluster to the standby cluster.
 * </p>
 */
public class SyncAndForwardModeImpl extends ReplicationModeImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SyncAndForwardModeImpl.class);

  protected SyncAndForwardModeImpl(ReplicationLogGroup logGroup) {
    super(logGroup);
  }

  @Override
  void onEnter() throws IOException {
    LOG.info("HAGroup {} entered mode {}", logGroup, this);
    // create a log on the standby cluster
    log = logGroup.createStandbyLog();
    log.init();
    // no-op if the forwarder is already started
    logGroup.getLogForwarder().start();
  }

  @Override
  void onExit(boolean gracefulShutdown) {
    LOG.info("HAGroup {} exiting mode {} graceful={}", logGroup, this, gracefulShutdown);
    // stop the replication log forwarding
    logGroup.getLogForwarder().stop();
    if (gracefulShutdown) {
      closeReplicationLog();
    } else {
      closeReplicationLogOnError();
    }
  }

  @Override
  ReplicationMode onFailure(Throwable e) throws IOException {
    LOG.info("HAGroup {} mode={} got error", logGroup, this, e);
    try {
      logGroup.setHAGroupStatusToStoreAndForward();
    } catch (Exception ex) {
      // Fatal error when we can't update the HAGroup status
      String message =
        String.format("HAGroup %s could not update status to STORE_AND_FORWARD", logGroup);
      LOG.error(message, ex);
      logGroup.abort(message, ex);
    }
    return STORE_AND_FORWARD;
  }

  @Override
  ReplicationMode getMode() {
    return SYNC_AND_FORWARD;
  }
}
