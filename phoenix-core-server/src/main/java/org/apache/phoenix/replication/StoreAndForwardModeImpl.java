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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Store and Forward mode implementation
 * <p>
 * This class implements the store and forward replication mode. It delegates the append and sync
 * events to the replication log on the fallback cluster. In the background, it also forwards the
 * replication log from the fallback cluster to the standby cluster.
 * </p>
 */
public class StoreAndForwardModeImpl extends ReplicationModeImpl {
  private static final Logger LOG = LoggerFactory.getLogger(StoreAndForwardModeImpl.class);

  // fraction of the zk session timeout to re-update the HA Group state to ACTIVE_NOT_IN_SYNC
  private static final double HA_GROUP_STORE_UPDATE_MULTIPLIER = 0.7;

  private ScheduledExecutorService haGroupStoreUpdateExecutor;

  protected StoreAndForwardModeImpl(ReplicationLogGroup logGroup) {
    super(logGroup);
  }

  @Override
  void onEnter() throws IOException {
    LOG.info("HAGroup {} entered mode {}", logGroup, this);
    // create a log on the fallback cluster
    log = logGroup.createFallbackLog();
    log.init();
    // Schedule task to periodically set the HAGroupStore state to ACTIVE_NOT_IN_SYNC
    startHAGroupStoreUpdateTask();
    // start the log forwarder
    logGroup.getLogForwarder().start();
  }

  private long getHAGroupStoreUpdateInterval() {
    return (long) Math.ceil(logGroup.conf.getLong(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT)
      * HA_GROUP_STORE_UPDATE_MULTIPLIER);
  }

  private void startHAGroupStoreUpdateTask() {
    long haGroupStoreUpdateInterval = getHAGroupStoreUpdateInterval();
    haGroupStoreUpdateExecutor =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setNameFormat("StoreAndForwardStatusUpdate-" + logGroup.getHAGroupName() + "-%d")
        .setDaemon(true).build());
    haGroupStoreUpdateExecutor.scheduleAtFixedRate(() -> {
      try {
        logGroup.setHAGroupStatusToStoreAndForward();
      } catch (Exception e) {
        // retry again in the next interval
        LOG.info("HAGroup {} failed to re-update the status to STORE_AND_FORWARD", logGroup, e);
      }
    }, 0, haGroupStoreUpdateInterval, TimeUnit.MILLISECONDS);
    LOG.info("HAGroup {} started haGroupStoreUpdateExecutor with interval {}ms", logGroup,
      haGroupStoreUpdateInterval);
  }

  private void stopHAGroupStoreUpdateTask() {
    if (haGroupStoreUpdateExecutor != null) {
      haGroupStoreUpdateExecutor.shutdown();
      try {
        if (!haGroupStoreUpdateExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          haGroupStoreUpdateExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        haGroupStoreUpdateExecutor.shutdownNow();
      }
      LOG.info("HAGroup {} stopped haGroupStoreUpdateExecutor ", logGroup);
    }
  }

  @Override
  void onExit(boolean gracefulShutdown) {
    LOG.info("HAGroup {} exiting mode {} graceful={}", logGroup, this, gracefulShutdown);
    stopHAGroupStoreUpdateTask();
    if (gracefulShutdown) {
      closeReplicationLog();
    } else {
      closeReplicationLogOnError();
    }
  }

  @Override
  ReplicationMode onFailure(Throwable e) throws IOException {
    // Treating failures in STORE_AND_FORWARD mode as fatal errors
    String message = String.format("HAGroup %s mode=%s got error", logGroup, this);
    LOG.error(message, e);
    logGroup.abort(message, e);
    // unreachable, we remain in the same mode
    return STORE_AND_FORWARD;
  }

  @Override
  ReplicationMode getMode() {
    return STORE_AND_FORWARD;
  }
}
