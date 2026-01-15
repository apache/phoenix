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
package org.apache.phoenix.replication.reader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This component is responsible to start/stop replication log replay via
 * {@link ReplicationLogReplay} for all the HA groups
 */
public class ReplicationLogReplayService {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogReplayService.class);

  /**
   * Configuration key for enabling/disabling replication replay service
   */
  public static final String PHOENIX_REPLICATION_REPLAY_ENABLED =
    "phoenix.replication.replay.enabled";

  /**
   * Default value for replication replay service enabled flag
   */
  public static final boolean DEFAULT_REPLICATION_REPLAY_ENABLED = false;

  /**
   * Number of threads in the executor pool for the replication replay service
   */
  public static final int REPLICATION_REPLAY_SERVICE_EXECUTOR_THREAD_COUNT = 1;

  /**
   * Configuration key for executor thread frequency in seconds
   */
  public static final String REPLICATION_REPLAY_SERVICE_EXECUTOR_THREAD_FREQUENCY_SECONDS_KEY =
    "phoenix.replication.replay.service.executor.frequency.seconds";

  /**
   * Default frequency in seconds for executor thread execution
   */
  public static final int DEFAULT_REPLICATION_REPLAY_SERVICE_EXECUTOR_THREAD_FREQUENCY_SECONDS = 60;

  /**
   * Configuration key for executor shutdown timeout in seconds
   */
  public static final String REPLICATION_REPLAY_SERVICE_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS_KEY =
    "phoenix.replication.replay.service.executor.shutdown.timeout.seconds";

  /**
   * Default shutdown timeout in seconds for graceful executor shutdown
   */
  public static final int DEFAULT_REPLICATION_REPLAY_SERVICE_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 30;

  private static volatile ReplicationLogReplayService instance;

  private final Configuration conf;
  private ScheduledExecutorService scheduler;
  private volatile boolean isRunning = false;

  private ReplicationLogReplayService(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * Gets the singleton instance of the ReplicationLogReplayService using the lazy initializer
   * pattern. Initializes the instance if it hasn't been created yet.
   * @param conf Configuration object.
   * @return The singleton ReplicationLogManager instance.
   * @throws IOException If initialization fails.
   */
  public static ReplicationLogReplayService getInstance(Configuration conf) throws IOException {
    if (instance == null) {
      synchronized (ReplicationLogReplayService.class) {
        if (instance == null) {
          instance = new ReplicationLogReplayService(conf);
        }
      }
    }
    return instance;
  }

  /**
   * Starts the replication log replay service by initializing the scheduler and scheduling periodic
   * replay operations for each HA Group.
   * @throws IOException if there's an error during initialization
   */
  public void start() throws IOException {
    boolean isEnabled =
      conf.getBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, DEFAULT_REPLICATION_REPLAY_ENABLED);
    if (!isEnabled) {
      LOG.info("Replication replay service is disabled. Skipping start operation.");
      return;
    }
    synchronized (this) {
      if (isRunning) {
        LOG.debug("ReplicationLogReplayService is already running");
        return;
      }
      int executorFrequencySeconds =
        conf.getInt(REPLICATION_REPLAY_SERVICE_EXECUTOR_THREAD_FREQUENCY_SECONDS_KEY,
          DEFAULT_REPLICATION_REPLAY_SERVICE_EXECUTOR_THREAD_FREQUENCY_SECONDS);
      // Initialize and schedule the executors
      scheduler = Executors.newScheduledThreadPool(REPLICATION_REPLAY_SERVICE_EXECUTOR_THREAD_COUNT,
        new ThreadFactoryBuilder().setNameFormat("ReplicationLogReplayService-%d").build());
      scheduler.scheduleAtFixedRate(() -> {
        try {
          startReplicationReplay();
        } catch (Exception e) {
          LOG.error("Error during trigger of start replication replay", e);
        }
      }, 0, executorFrequencySeconds, TimeUnit.SECONDS);
      isRunning = true;
      LOG.info("ReplicationLogReplayService started ");
    }
  }

  /**
   * Stops the replication log replay service by shutting down the scheduler gracefully. Waits for
   * the configured shutdown timeout before forcing shutdown if necessary.
   * @throws IOException if there's an error during shutdown
   */
  public void stop() throws IOException {
    boolean isEnabled =
      conf.getBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, DEFAULT_REPLICATION_REPLAY_ENABLED);
    if (!isEnabled) {
      LOG.info("Replication replay service is disabled. Skipping stop operation.");
      return;
    }
    ScheduledExecutorService schedulerToShutdown = null;
    synchronized (this) {
      if (!isRunning) {
        LOG.warn("ReplicationLogReplayService is not running");
        return;
      }
      isRunning = false;
      schedulerToShutdown = scheduler;
    }
    int executorShutdownTimeoutSeconds =
      conf.getInt(REPLICATION_REPLAY_SERVICE_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS_KEY,
        DEFAULT_REPLICATION_REPLAY_SERVICE_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
    if (schedulerToShutdown != null && !schedulerToShutdown.isShutdown()) {
      schedulerToShutdown.shutdown();
      try {
        if (
          !schedulerToShutdown.awaitTermination(executorShutdownTimeoutSeconds, TimeUnit.SECONDS)
        ) {
          schedulerToShutdown.shutdownNow();
        }
      } catch (InterruptedException e) {
        schedulerToShutdown.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    try {
      stopReplicationReplay();
    } catch (Exception exception) {
      LOG.error("Error while stopping the replication replay.", exception);
      // TODO: Should there be an exception thrown instead and halt the RS stop?
    }

    LOG.info("ReplicationLogReplayService stopped successfully");
  }

  /**
   * Start Replication Replay for all the HA groups
   */
  protected void startReplicationReplay() throws IOException, SQLException {
    List<String> replicationGroups = getReplicationGroups();
    for (String replicationGroup : replicationGroups) {
      ReplicationLogReplay.get(conf, replicationGroup).startReplay();
    }
  }

  /**
   * Stops Replication Replay for all the HA groups
   */
  protected void stopReplicationReplay() throws IOException, SQLException {
    List<String> replicationGroups = getReplicationGroups();
    for (String replicationGroup : replicationGroups) {
      ReplicationLogReplay replicationLogReplay = ReplicationLogReplay.get(conf, replicationGroup);
      replicationLogReplay.stopReplay();
      replicationLogReplay.close();
    }
  }

  /** Returns the list of HA groups on the cluster */
  protected List<String> getReplicationGroups() throws SQLException {
    return HAGroupStoreManager.getInstance(conf).getHAGroupNames();
  }
}
