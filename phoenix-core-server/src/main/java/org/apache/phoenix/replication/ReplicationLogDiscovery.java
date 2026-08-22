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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Abstract base class for discovering and processing replication log files in a round-by-round
 * manner. This class provides the core framework for: - Discovering replication log files from
 * configured directories (new files and in-progress files) - Processing files in time-based rounds
 * with configurable duration and buffer periods - Tracking progress via lastRoundProcessed to
 * enable resumable processing - Scheduling periodic replay operations via a configurable executor
 * service Round-based Processing: - Files are organized into replication rounds based on timestamps
 * - Each round represents a time window (e.g., 1 minute) of replication activity - Processing waits
 * for round completion + buffer time before processing to ensure all files are available Subclasses
 * must implement: - processFile(Path): Defines how individual replication log files are processed -
 * createMetricsSource(): Provides metrics tracking for monitoring - Configuration methods: Thread
 * counts, intervals, probabilities, etc. File Processing Flow: 1. Discover new files for the
 * current round 2. Mark files as in-progress (move to in-progress directory) 3. Process each file
 * via abstract processFile() method 4. Mark successfully processed files as completed (delete from
 * in-progress) 5. Update lastRoundProcessed to track progress
 */
public abstract class ReplicationLogDiscovery {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscovery.class);

  /**
   * Default number of threads in the executor pool for processing replication logs
   */
  private static final int DEFAULT_EXECUTOR_THREAD_COUNT = 1;

  /**
   * Default thread name format for executor threads
   */
  private static final String DEFAULT_EXECUTOR_THREAD_NAME_FORMAT = "ReplicationLogDiscovery-%d";

  /**
   * Default timeout in seconds for graceful shutdown of the executor service
   */
  private static final long DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 30;

  /**
   * Default probability (in percentage) for processing files from in-progress directory
   */
  private static final double DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY = 5.0;

  /**
   * Default buffer percentage for waiting time between processing rounds
   */
  protected static final double DEFAULT_WAITING_BUFFER_PERCENTAGE = 15.0;

  /**
   * Configuration key for maximum number of retries per in-progress file within a single processing
   * round. Files that fail this many times are skipped for the rest of the round.
   */
  public static final String REPLICATION_IN_PROGRESS_FILE_MAX_RETRIES_KEY =
    "phoenix.replication.in.progress.file.max.retries";

  public static final int DEFAULT_IN_PROGRESS_FILE_MAX_RETRIES = 1;

  /**
   * Configuration key for the minimum age (in seconds) of an in-progress file's rename timestamp
   * before it becomes eligible for processing. This prevents a file recently marked in-progress by
   * one region server from being immediately picked up by another.
   */
  public static final String REPLICATION_IN_PROGRESS_FILE_MIN_AGE_SECONDS_KEY =
    "phoenix.replication.in.progress.file.min.age.seconds";

  public static final int DEFAULT_IN_PROGRESS_FILE_MIN_AGE_SECONDS = 60;

  /**
   * Configuration key for the epsilon margin (milliseconds) added to the aligned scheduler wake
   * instant. The replay scheduler fires on a {@code System.nanoTime()} grid while the round
   * eligibility gate reads the wall clock ({@code EnvironmentEdgeManager.currentTime()}). Aligning
   * exactly to the eligibility instant lets a few ms of nanoTime-vs-wall-clock skew tip a wake-up
   * just below the boundary, which costs a full poll cycle. Waking epsilon after the eligibility
   * instant absorbs that skew.
   */
  public static final String REPLICATION_ALIGNED_DELAY_EPSILON_MILLIS_KEY =
    "phoenix.replication.discovery.aligned.delay.epsilon.millis";

  /**
   * Default epsilon margin in milliseconds. 500ms comfortably exceeds the small (single- to
   * low-tens-of-milliseconds) nanoTime-vs-wall-clock skew this margin absorbs, yet stays under 1%
   * of a 60s round, so best-case first-pickup latency is essentially unchanged. The margin is
   * absolute (it offsets clock skew, which does not scale with round duration), so for atypically
   * short custom round durations operators may lower it via
   * {@link #REPLICATION_ALIGNED_DELAY_EPSILON_MILLIS_KEY} to keep epsilon a small fraction of the
   * round.
   */
  public static final long DEFAULT_ALIGNED_DELAY_EPSILON_MILLIS = 500L;

  protected final Configuration conf;
  protected final String haGroupName;
  protected final ReplicationLogTracker replicationLogTracker;
  @GuardedBy("this")
  protected ScheduledExecutorService scheduler;
  protected volatile boolean isRunning = false;
  protected volatile ReplicationRound lastRoundProcessed;
  protected MetricsReplicationLogDiscovery metrics;
  protected long roundTimeMills;
  protected long bufferMillis;
  /**
   * Wall-clock instant (ms) of the round-eligibility grid point the most recently scheduled cycle
   * targets, or {@link Long#MIN_VALUE} before the first schedule of the current generation. Used by
   * {@link #scheduleNextReplay()} to guarantee each reschedule advances to a grid point strictly
   * after the previous one, so a wake landing exactly on (delay 0) or slightly before (nanoTime
   * skew) the boundary it just processed does not re-select the same grid point and run a redundant
   * cycle. Reset in {@link #start()} so a restarted generation re-anchors from scratch.
   */
  @GuardedBy("this")
  protected long lastAlignedTargetMillis = Long.MIN_VALUE;
  /**
   * One-shot guard so a misconfigured {@link #REPLICATION_ALIGNED_DELAY_EPSILON_MILLIS_KEY} logs
   * its fall-back WARN once rather than every scheduling cycle (the epsilon is read live per
   * cycle).
   */
  private final AtomicBoolean warnedInvalidEpsilon = new AtomicBoolean(false);

  public ReplicationLogDiscovery(final ReplicationLogTracker replicationLogTracker) {
    this.replicationLogTracker = replicationLogTracker;
    this.haGroupName = replicationLogTracker.getHaGroupName();
    this.conf = replicationLogTracker.getConf();
    this.roundTimeMills = replicationLogTracker.getReplicationShardDirectoryManager()
      .getReplicationRoundDurationSeconds() * 1000L;
    this.bufferMillis = (long) (roundTimeMills * getWaitingBufferPercentage() / 100.0);
  }

  public void init() throws IOException {
    LOG.info("Initializing ReplicationLogDiscovery for haGroup: {}", haGroupName);
    initializeLastRoundProcessed();
    this.metrics = createMetricsSource();
  }

  public void close() {
    replicationLogTracker.close();
    if (this.metrics != null) {
      this.metrics.close();
    }
  }

  /**
   * Starts the replication log discovery service. Creates a scheduler with the configured thread
   * count and launches a self-rescheduling one-shot replay chain (see
   * {@link #scheduleNextReplay()}) that re-anchors each replay to the aligned round-eligibility
   * grid every cycle, rather than firing at a fixed period.
   * @throws IOException if there's an error during initialization
   */
  public void start() throws IOException {
    synchronized (this) {
      if (isRunning) {
        LOG.warn("ReplicationLogDiscovery is already running for haGroup: {}", haGroupName);
        return;
      }
      // Single-shot rescheduling chain (see scheduleNextReplay). Discard any queued
      // (not-yet-started) delayed task on shutdown so stop() is deterministic and no replay
      // fires after we intend to stop.
      ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(getExecutorThreadCount(),
          new ThreadFactoryBuilder().setNameFormat(getExecutorThreadNameFormat()).build());
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
      scheduler = executor;
      isRunning = true;
      // Re-anchor the aligned-target guard so this fresh generation schedules from the next grid
      // point rather than being constrained by a target left over from a previous start()/stop().
      lastAlignedTargetMillis = Long.MIN_VALUE;
      try {
        scheduleNextReplay();
      } catch (RuntimeException | Error e) {
        // Scheduling the first cycle failed (e.g. a bad epsilon config value). Roll back so we
        // don't leave a live idle executor with isRunning==true (which reports healthy while
        // nothing polls) and so a later start() can retry cleanly.
        isRunning = false;
        scheduler = null;
        executor.shutdownNow();
        throw e;
      }
      LOG.info("ReplicationLogDiscovery started for haGroup: {}", haGroupName);
    }
  }

  /**
   * Stops the replication log discovery service by shutting down the scheduler gracefully. Waits
   * for the configured shutdown timeout before forcing shutdown if necessary.
   * @throws IOException if there's an error during shutdown
   */
  public void stop() {
    ScheduledExecutorService schedulerToShutdown;

    synchronized (this) {
      if (!isRunning) {
        LOG.warn("ReplicationLogDiscovery is not running for haGroup: {}", haGroupName);
        return;
      }

      isRunning = false;
      schedulerToShutdown = scheduler;
    }

    if (schedulerToShutdown != null && !schedulerToShutdown.isShutdown()) {
      schedulerToShutdown.shutdown();
      try {
        if (!schedulerToShutdown.awaitTermination(getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
          schedulerToShutdown.shutdownNow();
        }
      } catch (InterruptedException e) {
        schedulerToShutdown.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    LOG.info("ReplicationLogDiscovery stopped for haGroup: {}", haGroupName);
  }

  /**
   * Schedules the next replay as a single-shot task whose delay is recomputed each cycle via
   * {@link #computeAlignedInitialDelay()}. Recomputing every cycle re-pins each wake-up to the
   * wall-clock round-eligibility grid, correcting scheduler/wall-clock drift instead of letting a
   * one-time misalignment persist for the life of the process (which fixed-rate scheduling does).
   * All region servers still converge on the same grid, preserving PHOENIX-7813's shared wake-up.
   */
  @GuardedBy("this")
  protected void scheduleNextReplay() {
    long now = EnvironmentEdgeManager.currentTime();
    long delayMs = computeAlignedInitialDelay(now);
    // Guarantee the next fire targets a grid point strictly after the one the previous cycle
    // targeted. A wake landing exactly on (delayMs == 0) or slightly before (nanoTime skew, small
    // positive delay) the boundary we just processed would otherwise re-select the same grid point
    // and run a redundant cycle before the clock advances past it; bump one full round in that
    // case. targetMillis is derived from the same clock read as delayMs, so the two never skew.
    long targetMillis = now + delayMs;
    if (targetMillis <= lastAlignedTargetMillis) {
      delayMs += roundTimeMills;
      targetMillis += roundTimeMills;
    }
    lastAlignedTargetMillis = targetMillis;
    // Bind this cycle to the current scheduler generation. A stop()->start() restart
    // swaps in a new scheduler; a cycle launched on the old one must reschedule onto
    // that same (now shut-down) scheduler, not the new one.
    ScheduledExecutorService owner = scheduler;
    LOG.debug("Scheduling next replay for haGroup: {} in {}ms", haGroupName, delayMs);
    owner.schedule(() -> runReplayCycle(owner), delayMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Runs one replay pass and, unless the service has been stopped, schedules the next aligned pass.
   * A recoverable {@link Exception} from {@link #replay()} is logged and the chain continues, so a
   * single failed round does not break it. A fatal {@link Error} (OOM, stack overflow, linkage) is
   * logged, tears the chain down (marking the service not-running so the supervisor can rebuild
   * it), and is rethrown -- never rescheduled onto a potentially corrupted JVM. The reschedule is
   * guarded by the same lock stop() uses; if stop() shut the scheduler down first,
   * {@link #isRunning} is false and we do not reschedule (and a concurrent shutdown that rejects
   * the submission is caught and treated as "stop the chain").
   * @param owner the scheduler this cycle was launched on. If a stop()->start() restart has since
   *              swapped in a new scheduler, {@code owner} no longer equals {@link #scheduler} and
   *              this stale cycle must not reschedule onto the new generation (which would create a
   *              second concurrent chain and double the effective poll rate).
   */
  protected void runReplayCycle(ScheduledExecutorService owner) {
    try {
      replay();
    } catch (Exception e) {
      // Recoverable failure: log and fall through to reschedule so a single failed round does not
      // break the self-rescheduling chain.
      LOG.error("Error during replay for haGroup: {}", haGroupName, e);
    } catch (Error e) {
      // Fatal JVM condition (OutOfMemoryError, StackOverflowError, linkage failure). Do not swallow
      // it and do not reschedule another replay on a potentially corrupted JVM. Log it first --
      // otherwise the executor's discarded Future would hide it entirely -- then tear the chain
      // down exactly as a broken reschedule does below (mark the service not-running and shut this
      // executor down so the ReplicationLogReplayService supervisor can rebuild a fresh one) and
      // rethrow. Guarded on owner == scheduler so a stale cycle does not tear down a newer
      // generation's scheduler.
      LOG.error("Fatal error during replay for haGroup: {}; replay polling stopped, will be "
        + "restarted by the replay service supervisor", haGroupName, e);
      synchronized (this) {
        if (owner == scheduler) {
          isRunning = false;
          owner.shutdown();
        }
      }
      throw e;
    }
    // Reached only on normal completion or a caught (recoverable) Exception -- never after a fatal
    // Error, which propagates out above without rescheduling.
    synchronized (this) {
      if (isRunning && owner == scheduler) {
        try {
          scheduleNextReplay();
        } catch (RejectedExecutionException ree) {
          // benign: stop() shut the scheduler down between the guard check and submit
          LOG.debug("Scheduler shutting down, skipping reschedule for haGroup: {}", haGroupName);
        } catch (Throwable t) {
          // scheduleNextReplay() failed unexpectedly (something other than the benign
          // RejectedExecutionException handled above). The poll chain is already broken, so mark
          // the service not-running and shut down this now-idle executor. The
          // ReplicationLogReplayService supervisor re-invokes start() on its fixed-rate cadence
          // and, seeing isRunning==false, rebuilds a fresh executor -- self-healing instead of
          // silently wedging with isRunning==true (which would keep isRunning() reporting healthy
          // while nothing polls, and make every later start() no-op as "already running").
          LOG.error("Failed to schedule next replay for haGroup: {}; replay polling stopped, "
            + "will be restarted by the replay service supervisor", haGroupName, t);
          isRunning = false;
          owner.shutdown();
        }
      }
    }
  }

  /**
   * Executes a replay operation for the next set of replication rounds. This method continuously
   * retrieves and processes rounds using getNextRoundToProcess() until: - No more rounds are ready
   * to process (not enough time has elapsed), or - An error occurs during processing (will retry in
   * next scheduled run) For each round: 1. Calls processRound() to handle new files and optionally
   * in-progress files 2. Updates lastRoundProcessed to mark progress 3. Retrieves the next round to
   * process
   * @throws IOException if there's an error during replay processing
   */
  public void replay() throws IOException {
    Optional<ReplicationRound> optionalNextRound = getNextRoundToProcess();
    LOG.info("replay round={}", optionalNextRound.isPresent());
    while (optionalNextRound.isPresent()) {
      ReplicationRound replicationRound = optionalNextRound.get();
      try {
        processRound(replicationRound);
      } catch (IOException e) {
        LOG.error("Failed processing replication round {} for haGroup {}. Will retry"
          + "in next scheduled run.", replicationRound, haGroupName, e);
        break; // stop this run, retry later
      }
      setLastRoundProcessed(replicationRound);
      optionalNextRound = getNextRoundToProcess();
    }
    if (!optionalNextRound.isPresent()) {
      // no more rounds to process
      processNoMoreRoundsLeft();
    }
  }

  /**
   * Individual implementations can take specific actions when there are no more rounds ready to
   * process.
   */
  protected void processNoMoreRoundsLeft() throws IOException {
  }

  /**
   * Returns the next replication round to process based on lastRoundProcessed. Ensures sufficient
   * time (round duration + buffer) has elapsed before returning the next round.
   * @return Optional containing the next round to process, or empty if not enough time has passed
   */
  protected Optional<ReplicationRound> getNextRoundToProcess() {
    long lastRoundEndTimestamp = getLastRoundProcessed().getEndTime();
    long currentTime = EnvironmentEdgeManager.currentTime();
    LOG.info("last={} current={}", lastRoundEndTimestamp, currentTime);
    if (currentTime - lastRoundEndTimestamp < roundTimeMills + bufferMillis) {
      // nothing more to process
      return Optional.empty();
    }
    return Optional
      .of(new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills));
  }

  /**
   * Processes a single replication round by handling new files and optionally in-progress files.
   * Always processes new files for the round, and conditionally processes in-progress files based
   * on probability.
   * @param replicationRound - The replication round to process
   * @throws IOException if there's an error during round processing
   */
  protected void processRound(ReplicationRound replicationRound) throws IOException {
    LOG.info("Starting to process round: {} for haGroup: {}", replicationRound, haGroupName);
    // Increment the number of rounds processed
    getMetrics().incrementNumRoundsProcessed();

    // Process new files for the round
    processNewFilesForRound(replicationRound);
    if (shouldProcessInProgressDirectory()) {
      // Conditionally process the in progress files for the round
      processInProgressDirectory();
    }
    LOG.info("Finished processing round: {} for haGroup: {}", replicationRound, haGroupName);
  }

  /**
   * Determines whether to process in-progress directory files based on configured probability. Uses
   * random number generation to decide if in-progress files should be processed in this cycle.
   * @return true if in-progress directory should be processed, false otherwise
   */
  protected boolean shouldProcessInProgressDirectory() {
    return ThreadLocalRandom.current().nextDouble(100.0)
        < getInProgressDirectoryProcessProbability();
  }

  /**
   * Processes all new files for a specific replication round. Continuously processes files until no
   * new files remain for the round.
   * @param replicationRound - The replication round for which to process new files
   * @throws IOException if there's an error during file processing
   */
  protected void processNewFilesForRound(ReplicationRound replicationRound) throws IOException {
    LOG.info("Starting new files processing for round: {} for haGroup: {}", replicationRound,
      haGroupName);
    long startTime = EnvironmentEdgeManager.currentTime();
    List<Path> files = replicationLogTracker.getNewFilesForRound(replicationRound);
    LOG.info("Number of new files for round {} is {}", replicationRound, files.size());
    while (!files.isEmpty() && isRunning()) {
      processOneRandomFile(files);
      files = replicationLogTracker.getNewFilesForRound(replicationRound);
    }
    long duration = EnvironmentEdgeManager.currentTime() - startTime;
    LOG.info("Finished new files processing for round: {} in {}ms for haGroup: {}",
      replicationRound, duration, haGroupName);
    getMetrics().updateTimeToProcessNewFiles(duration);
    if (duration > roundTimeMills) {
      getMetrics().incrementRoundsExceedingRoundTime();
    }
  }

  /**
   * Processes all files in the in-progress directory whose rename timestamp is older than the
   * configured minimum age. Continuously processes files until no eligible in-progress files
   * remain.
   * @throws IOException if there's an error during file processing
   */
  protected void processInProgressDirectory() throws IOException {
    LOG.info("Starting {} directory processing for haGroup: {}",
      replicationLogTracker.getInProgressLogSubDirectoryName(), haGroupName);
    // Increase the count for number of times in progress directory is processed
    getMetrics().incrementNumInProgressDirectoryProcessed();
    long startTime = EnvironmentEdgeManager.currentTime();
    long renameTimestampThreshold =
      EnvironmentEdgeManager.currentTime() - getInProgressFileMinAgeSeconds() * 1000L;
    int maxRetries = getInProgressFileMaxRetries();
    Map<String, Integer> failureCount = new HashMap<>();
    List<Path> files = replicationLogTracker.getOlderInProgressFiles(renameTimestampThreshold);
    LOG.info("Number of {} files with renameTimestampThreshold {} is {} for haGroup: {}",
      replicationLogTracker.getInProgressLogSubDirectoryName(), renameTimestampThreshold,
      files.size(), haGroupName);
    while (!files.isEmpty() && isRunning()) {
      Optional<Path> failedFile = processOneRandomFile(files);
      if (failedFile.isPresent()) {
        String prefix = replicationLogTracker.getFilePrefix(failedFile.get());
        int count = failureCount.merge(prefix, 1, Integer::sum);
        if (count >= maxRetries) {
          LOG.warn(
            "File {} (prefix: {}) has failed {} time(s), reached max retries ({}). "
              + "Skipping for the rest of this round for haGroup: {}",
            failedFile.get(), prefix, count, maxRetries, haGroupName);
        }
      }
      renameTimestampThreshold =
        EnvironmentEdgeManager.currentTime() - getInProgressFileMinAgeSeconds() * 1000L;
      files = replicationLogTracker.getOlderInProgressFiles(renameTimestampThreshold);
      files.removeIf(
        f -> failureCount.getOrDefault(replicationLogTracker.getFilePrefix(f), 0) >= maxRetries);
    }
    long duration = EnvironmentEdgeManager.currentTime() - startTime;
    LOG.info("Finished in-progress files processing in {}ms for haGroup: {}", duration,
      haGroupName);
    getMetrics().updateTimeToProcessInProgressFiles(duration);
  }

  /**
   * Processes a single random file from the provided list. Marks the file as in-progress, processes
   * it, and marks it as completed or failed.
   * @param files - List of files from which to select and process one randomly
   * @return the original path of the file that failed, or empty if processing succeeded
   */
  private Optional<Path> processOneRandomFile(final List<Path> files) throws IOException {
    // Pick a random file and process it
    Path file = files.get(ThreadLocalRandom.current().nextInt(files.size()));
    Optional<Path> optionalInProgressFilePath = Optional.empty();
    try {
      optionalInProgressFilePath = replicationLogTracker.markInProgress(file);
      if (optionalInProgressFilePath.isPresent()) {
        processFile(optionalInProgressFilePath.get());
        replicationLogTracker.markCompleted(optionalInProgressFilePath.get());
      }
    } catch (IOException exception) {
      LOG.error("Failed to process the file {}", file, exception);
      optionalInProgressFilePath.ifPresent(replicationLogTracker::markFailed);
      // Not throwing this exception because next time another random file will be retried.
      return Optional.of(file);
    }
    return Optional.empty();
  }

  /**
   * Handles the processing of a single file.
   * @param path - The file to be processed
   * @throws IOException if there's an error during file processing
   */
  protected abstract void processFile(Path path) throws IOException;

  /** Creates a new metrics source for monitoring operations. */
  protected abstract MetricsReplicationLogDiscovery createMetricsSource();

  /**
   * Initializes lastRoundProcessed, sampling the current time up front to use as the no-files
   * fallback (see {@link #initializeLastRoundProcessed(long)}). Sampling here - before the file
   * scans - keeps the fallback anchored to when initialization began.
   * @throws IOException if there's an error reading file timestamps
   */
  protected void initializeLastRoundProcessed() throws IOException {
    initializeLastRoundProcessed(EnvironmentEdgeManager.currentTime());
  }

  /**
   * Initializes lastRoundProcessed based on minimum timestamp from 1. In-progress files (highest
   * priority) - indicates partially processed rounds 2. New files (medium priority) - indicates
   * unprocessed rounds waiting to be replayed 3. fallbackCurrentTime - used when no files exist,
   * starts from the supplied time The minimum timestamp is converted to a replication round using
   * getReplicationRoundFromEndTime(), which rounds down to the nearest round boundary to ensure we
   * start from a complete round.
   * @param fallbackCurrentTime the timestamp to start from when no files exist; sampled by the
   *                            caller at the start of initialization rather than re-read here, so a
   *                            slow init (or a state transition during init) cannot push the
   *                            starting round forward
   * @throws IOException if there's an error reading file timestamps
   */
  protected void initializeLastRoundProcessed(long fallbackCurrentTime) throws IOException {
    Optional<Long> minTimestampFromInProgressFiles = getMinTimestampFromInProgressFiles();
    if (minTimestampFromInProgressFiles.isPresent()) {
      LOG.info(
        "Initializing lastRoundProcessed for haGroup: {} from {} files with minimum "
          + "timestamp as {}",
        haGroupName, replicationLogTracker.getInProgressLogSubDirectoryName(),
        minTimestampFromInProgressFiles.get());
      this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
        .getReplicationRoundFromEndTime(minTimestampFromInProgressFiles.get());
    } else {
      Optional<Long> minTimestampFromNewFiles = getMinTimestampFromNewFiles();
      if (minTimestampFromNewFiles.isPresent()) {
        LOG.info(
          "Initializing lastRoundProcessed for haGroup: {} from {}"
            + "files with minimum timestamp as {}",
          haGroupName, replicationLogTracker.getInSubDirectoryName(),
          minTimestampFromNewFiles.get());
        this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
          .getReplicationRoundFromEndTime(minTimestampFromNewFiles.get());
      } else {
        LOG.info("Initializing lastRoundProcessed for haGroup: {} from current time {}",
          haGroupName, fallbackCurrentTime);
        this.lastRoundProcessed = replicationLogTracker.getReplicationShardDirectoryManager()
          .getReplicationRoundFromEndTime(fallbackCurrentTime);
      }
    }
  }

  /**
   * Get minimum timestamp from in progress files. If no in progress files, return empty.
   * @return minimum timestamp from in progress files.
   */
  protected Optional<Long> getMinTimestampFromInProgressFiles() throws IOException {
    List<Path> inProgressFiles = replicationLogTracker.getInProgressFiles();
    if (inProgressFiles.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(getMinTimestampFromFiles(inProgressFiles));
  }

  /**
   * Get minimum timestamp from new files. If no new files, return empty.
   * @return minimum timestamp from new files.
   */
  protected Optional<Long> getMinTimestampFromNewFiles() throws IOException {
    List<Path> newFiles = replicationLogTracker.getNewFiles();
    if (newFiles.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(getMinTimestampFromFiles(newFiles));
  }

  private long getMinTimestampFromFiles(List<Path> files) {
    long minTimestamp = org.apache.hadoop.hbase.util.EnvironmentEdgeManager.currentTime();
    for (Path file : files) {
      minTimestamp = Math.min(minTimestamp, replicationLogTracker.getFileTimestamp(file));
    }
    return minTimestamp;
  }

  /**
   * Returns the executor thread count. Subclasses can override this method to provide custom name
   * format.
   * @return the executor thread count (default: 1).
   */
  public int getExecutorThreadCount() {
    return DEFAULT_EXECUTOR_THREAD_COUNT;
  }

  /**
   * Returns the executor thread name format. Subclasses can override this method to provide custom
   * name format.
   * @return the executor thread name format (default: ReplicationLogDiscovery-%d).
   */
  public String getExecutorThreadNameFormat() {
    return DEFAULT_EXECUTOR_THREAD_NAME_FORMAT;
  }

  /**
   * Returns the shutdown timeout in seconds. Subclasses can override this method to provide custom
   * timeout values.
   * @return The shutdown timeout in seconds (default: 30 seconds).
   */
  public long getShutdownTimeoutSeconds() {
    return DEFAULT_SHUTDOWN_TIMEOUT_SECONDS;
  }

  /**
   * Returns the probability (in percentage) for processing files from in-progress directory.
   * Subclasses can override this method to provide custom probabilities.
   * @return The probability (default 5.0%)
   */
  public double getInProgressDirectoryProcessProbability() {
    return DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY;
  }

  /**
   * Returns the buffer percentage for calculating buffer time. Subclasses can override this method
   * to provide custom buffer percentages.
   * @return The buffer percentage (default 15.0%)
   */
  public double getWaitingBufferPercentage() {
    return DEFAULT_WAITING_BUFFER_PERCENTAGE;
  }

  /**
   * Computes initial delay to align the scheduler to round-eligible boundaries so all RS wake up at
   * the same wall-clock moment. A round becomes eligible when currentTime >= roundEndTime +
   * bufferMillis, and rounds repeat every roundTimeMills. This gives a universal grid of eligible
   * ticks at bufferMillis + epsilon, bufferMillis + epsilon + roundTimeMills, bufferMillis +
   * epsilon + 2*roundTimeMills, etc. from epoch. All RS compute the same grid regardless of when
   * start() is called.
   * @return the initial delay in milliseconds until the next round-eligible tick
   */
  protected long computeAlignedInitialDelay() {
    return computeAlignedInitialDelay(EnvironmentEdgeManager.currentTime());
  }

  /**
   * Overload of {@link #computeAlignedInitialDelay()} that aligns against a caller-supplied
   * {@code now}, so a caller can derive both the delay and the absolute target grid instant
   * ({@code now + delay}) from a single clock read without the two skewing across reads.
   * @param now the reference wall-clock instant in milliseconds
   * @return the delay in milliseconds until the next round-eligible tick at or after {@code now}
   */
  protected long computeAlignedInitialDelay(long now) {
    // Anchor epsilon past the eligibility instant (bufferMillis past a round line) so that a
    // scheduler firing slightly early (nanoTime skew) still clears the wall-clock gate.
    long anchor = bufferMillis + getAlignedDelayEpsilonMillis();
    long elapsed = Math.floorMod(now - anchor, roundTimeMills);
    return (elapsed == 0) ? 0 : roundTimeMills - elapsed;
  }

  public int getInProgressFileMaxRetries() {
    return conf.getInt(REPLICATION_IN_PROGRESS_FILE_MAX_RETRIES_KEY,
      DEFAULT_IN_PROGRESS_FILE_MAX_RETRIES);
  }

  public int getInProgressFileMinAgeSeconds() {
    return conf.getInt(REPLICATION_IN_PROGRESS_FILE_MIN_AGE_SECONDS_KEY,
      DEFAULT_IN_PROGRESS_FILE_MIN_AGE_SECONDS);
  }

  /**
   * Returns the epsilon margin (milliseconds) added to the aligned scheduler wake instant. Guards
   * against a misconfigured value: a non-numeric string, a negative value (which would move wakes
   * <em>before</em> eligibility and reintroduce missed rounds), or a value
   * {@code >= roundTimeMills} (which wraps through {@link Math#floorMod} and no longer represents
   * the documented "epsilon after the boundary") all fall back to
   * {@link #DEFAULT_ALIGNED_DELAY_EPSILON_MILLIS} with a one-shot WARN. Riding over the
   * misconfiguration keeps replay polling on a safe default rather than wedging the group, while
   * the WARN makes the bad config visible.
   * @return the epsilon margin in milliseconds, always within {@code [0, roundTimeMills)}.
   */
  public long getAlignedDelayEpsilonMillis() {
    long epsilon;
    try {
      epsilon = conf.getLong(REPLICATION_ALIGNED_DELAY_EPSILON_MILLIS_KEY,
        DEFAULT_ALIGNED_DELAY_EPSILON_MILLIS);
    } catch (NumberFormatException e) {
      // Hadoop's getLong throws (rather than returning the default) when the key is present but
      // not parseable as a number.
      warnInvalidEpsilon(
        "non-numeric value \"" + conf.get(REPLICATION_ALIGNED_DELAY_EPSILON_MILLIS_KEY) + "\"");
      return DEFAULT_ALIGNED_DELAY_EPSILON_MILLIS;
    }
    if (epsilon < 0 || epsilon >= roundTimeMills) {
      warnInvalidEpsilon(epsilon + "ms");
      return DEFAULT_ALIGNED_DELAY_EPSILON_MILLIS;
    }
    return epsilon;
  }

  private void warnInvalidEpsilon(String badValueDescription) {
    if (warnedInvalidEpsilon.compareAndSet(false, true)) {
      LOG.warn(
        "Invalid {} ({}) for haGroup: {}; must be within [0, {}). Falling back to default {}ms.",
        REPLICATION_ALIGNED_DELAY_EPSILON_MILLIS_KEY, badValueDescription, haGroupName,
        roundTimeMills, DEFAULT_ALIGNED_DELAY_EPSILON_MILLIS);
    }
  }

  public ReplicationLogTracker getReplicationLogFileTracker() {
    return this.replicationLogTracker;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public String getHaGroupName() {
    return this.haGroupName;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public ReplicationRound getLastRoundProcessed() {
    return lastRoundProcessed;
  }

  public void setLastRoundProcessed(final ReplicationRound replicationRound) {
    this.lastRoundProcessed = replicationRound;
  }

  public MetricsReplicationLogDiscovery getMetrics() {
    return this.metrics;
  }
}
