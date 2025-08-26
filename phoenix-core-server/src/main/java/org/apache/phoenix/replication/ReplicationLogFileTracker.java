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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogFileTracker;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for tracking and managing replication log files across different states.
 * Handles file lifecycle management including new files, in-progress files, and completed files.
 *
 * This will be extended by specific implementations for tracking IN and OUT directory files.
 */
public abstract class ReplicationLogFileTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogFileTracker.class);

    /**
     * Configuration key for number of retries when deleting files
     */
    private static final String FILE_DELETE_RETRIES_KEY = "phoenix.replication.file.delete.retries";

    /**
     * Default number of retries for file deletion operations
     */
    private static final int DEFAULT_FILE_DELETE_RETRIES = 3;

    /**
     * Configuration key for delay between file deletion retry attempts
     */
     private static final String FILE_DELETE_RETRY_DELAY_MS_KEY = 
         "phoenix.replication.file.delete.retry.delay.ms";

     /**
     * Default delay in milliseconds between file deletion retry attempts
     */
    private static final long DEFAULT_FILE_DELETE_RETRY_DELAY_MS = 1000L;

    private final URI rootURI;
    private final FileSystem fileSystem;
    private Path inProgressDirPath;
    private ReplicationShardDirectoryManager replicationShardDirectoryManager;
    protected final Configuration conf;
    protected final String haGroupName;
    protected MetricsReplicationLogFileTracker metrics;

    protected ReplicationLogFileTracker(final Configuration conf, final String haGroupName, 
        final FileSystem fileSystem, final URI rootURI) {
        this.conf = conf;
        this.fileSystem = fileSystem;
        this.haGroupName = haGroupName;
        this.rootURI = rootURI;
    }

    protected abstract String getNewLogSubDirectoryName();

    /** Creates a new metrics source for monitoring operations. */
    protected abstract MetricsReplicationLogFileTracker createMetricsSource();

    protected String getInProgressLogSubDirectoryName() {
        return getNewLogSubDirectoryName() + "_progress";
    }

    /**
     * Initializes the file tracker by setting up new files directory and in-progress directory.
     * Creates the in-progress directory (rootURI/<group-name>/[in/out]_progress) if it doesn't 
     * exist.
     */
    public void init() throws IOException {
        Path newFilesDirectory = new Path(new Path(rootURI.getPath(), haGroupName), 
            getNewLogSubDirectoryName());
        this.replicationShardDirectoryManager = new ReplicationShardDirectoryManager(conf, 
            newFilesDirectory);
        this.inProgressDirPath = new Path(new Path(rootURI.getPath(), haGroupName), 
            getInProgressLogSubDirectoryName());
        createDirectoryIfNotExists(inProgressDirPath);
        this.metrics = createMetricsSource();
    }

    public void close() {
        if(this.metrics != null) {
            this.metrics.close();
        }
    }

    /**
     * Retrieves new replication log files that belong to a specific replication round. It skips 
     * the 
     * invalid files (if any)
     * @param replicationRound - The replication round for which to retrieve files
     * @return List of valid log file paths that belong to the specified replication round
     * @throws IOException if there's an error accessing the file system
     */
    protected List<Path> getNewFilesForRound(ReplicationRound replicationRound) throws IOException {
        Path roundDirectory = replicationShardDirectoryManager.getShardDirectory(replicationRound);
        LOG.info("Getting new files for round {} from shard {}", replicationRound, roundDirectory);
        if (!fileSystem.exists(roundDirectory)) {
            return Collections.emptyList();
        }

        // List the files in roundDirectory
        FileStatus[] fileStatuses = fileSystem.listStatus(roundDirectory);
        LOG.info("Number of new files found {}", fileStatuses.length);
        List<Path> filesInRound = new ArrayList<>();

        // Filter the files belonging to current round
        for (FileStatus status : fileStatuses) {
            if(status.isFile()) {
                if (!isValidLogFile(status.getPath())) {
                    LOG.warn("Invalid log file found at {}", status.getPath());
                    continue; // Skip invalid files
                }
                try {
                    long fileTimestamp = getFileTimestamp(status.getPath());
                    if(fileTimestamp >= replicationRound.getStartTime() && 
                        fileTimestamp <= replicationRound.getEndTime()) {
                        filesInRound.add(status.getPath());
                    }
                } catch (NumberFormatException exception) {
                    // Should we throw an exception here instead?
                    LOG.warn("Failed to extract timestamp from {}. Ignoring the file.", 
                        status.getPath());
                }
            }
        }
        return filesInRound;
    }

    /**
     * Retrieves all valid log files currently in the in-progress directory.
     * @return List of valid log file paths in the in-progress directory, empty list if directory 
     * doesn't exist
     * @throws IOException if there's an error accessing the file system
     */
    protected List<Path> getInProgressFiles() throws IOException {
        if (!fileSystem.exists(getInProgressDirPath())) {
            return Collections.emptyList();
        }

        FileStatus[] fileStatuses = fileSystem.listStatus(getInProgressDirPath());
        List<Path> inProgressFiles = new ArrayList<>();

        for (FileStatus status : fileStatuses) {
            if (status.isFile() && isValidLogFile(status.getPath())) {
                inProgressFiles.add(status.getPath());
            }
        }

        return inProgressFiles;
    }

    /**
     * Retrieves all valid log files from all shard directories.
     * @return List of all valid log file paths from all shard directories
     * @throws IOException if there's an error accessing the file system
     */
    protected List<Path> getNewFiles() throws IOException {
        List<Path> shardPaths = replicationShardDirectoryManager.getAllShardPaths();
        List<Path> newFiles = new ArrayList<>();
        for(Path shardPath : shardPaths) {
            if(fileSystem.exists(shardPath)) {
                FileStatus[] fileStatuses = fileSystem.listStatus(shardPath);
                for(FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isFile() && isValidLogFile(fileStatus.getPath())) {
                        newFiles.add(fileStatus.getPath());
                    }
                }
            }
        }
        return newFiles;
    }

    /**
     * Marks a file as completed by deleting it from the file system. Uses retry logic with 
     * configurable retry count and delay. During retry attempts, it fetches the file with same 
     * prefix (instead of re-using the same file) because it would likely be re-named by some 
     * other process
     * @param file - The file path to mark as completed
     * @return true if file was successfully deleted, false otherwise
     */
    protected boolean markCompleted(final Path file) {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        // Increment the metrics count
        getMetrics().incrementMarkFileCompletedRequestCount();

        int maxRetries = conf.getInt(FILE_DELETE_RETRIES_KEY, DEFAULT_FILE_DELETE_RETRIES);
        long retryDelayMs = conf.getLong(FILE_DELETE_RETRY_DELAY_MS_KEY, 
            DEFAULT_FILE_DELETE_RETRY_DELAY_MS);

        Path fileToDelete = file;
        final String filePrefix = getFilePrefix(fileToDelete);

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (fileSystem.delete(fileToDelete, false)) {
                    LOG.info("Successfully deleted completed file: {}", fileToDelete);
                    long endTime = EnvironmentEdgeManager.currentTimeMillis();
                    getMetrics().updateMarkFileCompletedTime(endTime - startTime);
                    return true;
                } else {
                    LOG.warn("Failed to delete file (attempt {}): {}", attempt + 1, fileToDelete);
                }
            } catch (IOException e) {
                LOG.warn("IOException while deleting file (attempt {}): {}", attempt + 1, 
                    fileToDelete, e);
            }

            // Increment the deletion failure count
            getMetrics().incrementMarkFileCompletedRequestFailedCount();

                // If deletion failed and it's not the last attempt, sleep first then try to find 
                // matching in-progress file
            if (attempt < maxRetries) {
                // Sleep before next retry
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting to retry file deletion: {}", file);
                    return false;
                }

                try {
                    // List in-progress files and find matching file with same 
                    // <timestamp>_<region-server> prefix
                    List<Path> inProgressFiles = getInProgressFiles();
                    List<Path> matchingFiles = inProgressFiles.stream()
                        .filter(path -> getFilePrefix(path).equals(filePrefix))
                        .collect(Collectors.toList());
                    // Assert only single file exists with that prefix
                    if (matchingFiles.size() == 1) {
                        Path matchingFile = matchingFiles.get(0);
                        LOG.info("Found matching in-progress file: {} for original file: {}", 
                            matchingFile, file);
                        // Update fileToDelete to the matching file for subsequent retries
                        fileToDelete = matchingFile;
                    } else if (matchingFiles.size() > 1) {
                        LOG.warn("Multiple matching in-progress files found for prefix {}: {}", 
                            filePrefix, matchingFiles.size());
                        long endTime = EnvironmentEdgeManager.currentTimeMillis();
                        getMetrics().updateMarkFileCompletedTime(endTime - startTime);
                        return false;
                    } else {
                        LOG.warn("No matching in-progress file found for prefix: {}. File must " +
                            "have " +
                            "been deleted by some other process.", filePrefix);
                        long endTime = EnvironmentEdgeManager.currentTimeMillis();
                        getMetrics().updateMarkFileCompletedTime(endTime - startTime);
                        return true;
                    }
                } catch (IOException e) {
                    LOG.warn("IOException while searching for matching in-progress file (attempt {}): " +
                        "{}", 
                        attempt + 1, file, e);
                }
            }
        }

        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        getMetrics().updateMarkFileCompletedTime(endTime - startTime);

        LOG.error("Failed to delete file after {} attempts: {}", maxRetries + 1, fileToDelete);
        return false;
    }

    /**
     * Marks a file as in-progress by renaming it with a UUID and moving to in-progress directory.
     * If file is already in in-progress directory, only updates the UUID.
     * @param file - The file path to mark as in progress
     * @return Optional value of renamed path if file rename was successful, else Optional.empty()
     */
    protected Optional<Path> markInProgress(final Path file) {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        try {

            final String fileName = file.getName();
            final String newFileName;
            final Path targetDirectory;

            // Check if file is already in in-progress directory
            if(file.getParent().toUri().getPath().equals(getInProgressDirPath().toString())) {
                // File is already in in-progress directory, replace UUID with a new one
                // keep the directory same as in progress
                String[] parts = fileName.split("_");
                // Remove the last part (UUID) and add new UUID
                StringBuilder newNameBuilder = new StringBuilder();
                for (int i = 0; i < parts.length - 1; i++) {
                    if (i > 0) {
                        newNameBuilder.append("_");
                    }
                    newNameBuilder.append(parts[i]);
                }
                String extension = fileName.substring(fileName.lastIndexOf("."));
                newNameBuilder.append("_").append(UUID.randomUUID().toString()).append(extension);
                newFileName = newNameBuilder.toString();
                targetDirectory = file.getParent();
            } else {
                // File is not in in-progress directory, add UUID and move to IN_PROGRESS directory
                String baseName = fileName.substring(0, fileName.lastIndexOf("."));
                String extension = fileName.substring(fileName.lastIndexOf("."));
                newFileName = baseName + "_" + UUID.randomUUID().toString() + extension;
                targetDirectory = getInProgressDirPath();
            }

            Path newPath = new Path(targetDirectory, newFileName);
            if (fileSystem.rename(file, newPath)) {
                LOG.debug("Successfully marked file as in progress: {} -> {}", file.getName(), 
                newFileName);
                return Optional.of(newPath);
            } else {
                LOG.warn("Failed to rename file for in-progress marking: {}", file);
                return Optional.empty();
            }
        } catch (IOException e) {
            LOG.error("IOException while marking file as in progress: {}", file, e);
            return Optional.empty();
        } finally {
            // Update the metrics
            getMetrics().incrementMarkFileInProgressRequestCount();
            long endTime =  EnvironmentEdgeManager.currentTimeMillis();
            getMetrics().updateMarkFileInProgressTime(endTime - startTime);
        }
    }

    /**
     * Validates if a file is a valid log file by checking if it ends with ".plog" extension.
     * @param file - The file path to validate.
     * @return true if file format is correct, else false
     */
    protected boolean isValidLogFile(Path file) {
        final String fileName = file.getName();
        return fileName.endsWith(".plog");
    }

    /**
     * Extracts the timestamp from a log file name.
     * Assumes timestamp is the first part of the filename separated by underscore.
     * @param file - The file path to extract timestamp from.
     * return the timestamp from the file name
     */
    public long getFileTimestamp(Path file) throws NumberFormatException {
        String[] parts = file.getName().split("_");
        return Long.parseLong(parts[0]);
    }

    /**
     * Extracts the UUID from a log file name.
     * Assumes UUID is the last part of the filename before the extension.
     * @param file - The file path to extract UUID from.
     * @return Optional of UUID if file was in progress, else Optional.empty()
     */
    protected Optional<String> getFileUUID(Path file) throws NumberFormatException {
        String[] parts = file.getName().split("_");
        if(parts.length < 3) {
            return Optional.empty();
        }
        return Optional.of(parts[parts.length-1].split("\\.")[0]);
    }

    /**
     * Extracts everything except the UUID (last part) from a file path.
     * For example, from "1704153600000_rs1_12345678-1234-1234-1234-123456789abc.plog"
     * returns "1704153600000_rs1"
     * @param file - The file path to extract prefix from.
     */
    protected String getFilePrefix(Path file) {
        String fileName = file.getName();
        String[] parts = fileName.split("_");
        if (parts.length < 3) {
            return fileName.split("\\.")[0]; // Return full filename if no underscore found
        }

        // Return everything except the last part (UUID)
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            if (i > 0) {
                prefix.append("_");
            }
            prefix.append(parts[i]);
        }

        return prefix.toString();
    }

    /**
     * No op implementation for marking a file as failed
     * @param file - The file which needs to be marked as failed
     * @return - true if marked as failed, false otherwise
     */
    public boolean markFailed(final Path file) {
        getMetrics().incrementMarkFileFailedRequestCount();
        return true;
    }

    public FileSystem getFileSystem() {
        return this.fileSystem;
    }

    public ReplicationShardDirectoryManager getReplicationShardDirectoryManager() {
        return this.replicationShardDirectoryManager;
    }

    protected String getHaGroupName() {
        return this.haGroupName;
    }

    protected Configuration getConf() {
        return this.conf;
    }

    protected Path getInProgressDirPath() {
        return this.inProgressDirPath;
    }

    protected MetricsReplicationLogFileTracker getMetrics() {
        return this.metrics;
    }

    /**
     * Creates a directory if it doesn't exist.
     */
    private void createDirectoryIfNotExists(Path directoryPath) throws IOException {
        if (!fileSystem.exists(directoryPath)) {
            LOG.info("Creating directory {}", directoryPath);
            if (!fileSystem.mkdirs(directoryPath)) {
                throw new IOException("Failed to create directory: " + directoryPath);
            }
        }
    }
}
