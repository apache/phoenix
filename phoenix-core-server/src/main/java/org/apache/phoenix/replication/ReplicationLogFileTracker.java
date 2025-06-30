package org.apache.phoenix.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class ReplicationLogFileTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogFileTracker.class);
    
    // Singleton instances per haGroupName
    private static final Map<String, ReplicationLogFileTracker> instances = new ConcurrentHashMap<>();
    
    // Configuration keys for file operations
    private static final String FILE_DELETE_RETRIES_KEY = "phoenix.replication.file.delete.retries";
    private static final int DEFAULT_FILE_DELETE_RETRIES = 3;
    private static final String FILE_DELETE_RETRY_DELAY_MS_KEY = "phoenix.replication.file.delete.retry.delay.ms";
    private static final long DEFAULT_FILE_DELETE_RETRY_DELAY_MS = 1000L;

    private final Configuration conf;
    private final String haGroupName;
    private final URI rootURI;
    private final FileSystem fileSystem;
    private Path inProgressDirPath;
    private ReplicationShardDirectoryManager replicationShardDirectoryManager;

    protected ReplicationLogFileTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) {
        this.conf = conf;
        this.fileSystem = fileSystem;
        this.haGroupName = haGroupName;
        this.rootURI = rootURI;
    }

    protected abstract String getNewLogSubDirectoryName();

    protected String getInProgressLogSubDirectoryName() {
        return getNewLogSubDirectoryName() + "_progress";
    }

    public void init() throws IOException {
        Path newFilesDirectory = new Path(new Path(rootURI.getPath(), getNewLogSubDirectoryName()), haGroupName);
        this.replicationShardDirectoryManager = new ReplicationShardDirectoryManager(conf, newFilesDirectory);
        this.inProgressDirPath = new Path(new Path(rootURI.getPath(), getInProgressLogSubDirectoryName()), this.haGroupName);
        createDirectoryIfNotExists(inProgressDirPath);
    }

    protected List<Path> getNewFilesForRound(ReplicationRound replicationRound) throws IOException {
        Path roundDirectory = replicationShardDirectoryManager.getShardDirectory(replicationRound);
        System.out.println("Getting new files for round: " + replicationRound.getStartTime() + " - " + roundDirectory.toString());
        if (!fileSystem.exists(roundDirectory)) {
            return Collections.emptyList();
        }

        // List the files in roundDirectory
        FileStatus[] fileStatuses = fileSystem.listStatus(roundDirectory);
        System.out.println("Number of files found " + fileStatuses.length);
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
                    if(fileTimestamp >= replicationRound.getStartTime() && fileTimestamp <= replicationRound.getEndTime()) {
                        filesInRound.add(status.getPath());
                    }
                } catch (NumberFormatException exception) {
                    // Should we throw an exception here instead?
                    LOG.warn("Failed to extract timestamp from {}. Ignoring the file.", status.getPath());
                }
            }
        }
        return filesInRound;
    }

    protected List<Path> getInProgressFiles() throws IOException {
        if (!fileSystem.exists(inProgressDirPath)) {
            return Collections.emptyList();
        }

        FileStatus[] fileStatuses = fileSystem.listStatus(inProgressDirPath);
        List<Path> inProgressFiles = new ArrayList<>();

        for (FileStatus status : fileStatuses) {
            if (status.isFile() && isValidLogFile(status.getPath())) {
                inProgressFiles.add(status.getPath());
            }
        }

        return inProgressFiles;
    }

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

    protected boolean markCompleted(final Path file) {
        System.out.println("Mark Completed Method Called for " + file.toString());
        
        int maxRetries = conf.getInt(FILE_DELETE_RETRIES_KEY, DEFAULT_FILE_DELETE_RETRIES);
        long retryDelayMs = conf.getLong(FILE_DELETE_RETRY_DELAY_MS_KEY, DEFAULT_FILE_DELETE_RETRY_DELAY_MS);


        Path fileToDelete = file;
        final String filePrefix = getFilePrefix(fileToDelete);

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            System.out.println("For attempt " + attempt + " deleting " + fileToDelete.toUri());
            try {
                if (fileSystem.delete(fileToDelete, false)) {
                    System.out.println("Successfully deleted completed file: " + fileToDelete);
                    LOG.info("Successfully deleted completed file: {}", fileToDelete);
                    return true;
                } else {
                    LOG.warn("Failed to delete file (attempt {}): {}", attempt + 1, fileToDelete);
                }
            } catch (IOException e) {
                LOG.warn("IOException while deleting file (attempt {}): {}", attempt + 1, fileToDelete, e);
            }
            
            // If deletion fails and it's not the last attempt, sleep first then try to find matching in-progress file
            if (attempt < maxRetries) {
                // Sleep before next retry
                try {
                    System.out.println("Starting sleep");
                    Thread.sleep(retryDelayMs);
                    System.out.println("Stopping sleep");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting to retry file deletion: {}", file);
                    return false;
                }
                
                try {
                    // List in-progress files and find matching file with same <timestamp>_<region-server> prefix
                    List<Path> inProgressFiles = getInProgressFiles();
                    List<Path> matchingFiles = inProgressFiles.stream().filter(path -> getFilePrefix(path).equals(filePrefix)).collect(Collectors.toList());
                    // Assert only single file exists with that prefix
                    if (matchingFiles.size() == 1) {
                        Path matchingFile = matchingFiles.get(0);
                        LOG.info("Found matching in-progress file: {} for original file: {}", matchingFile, file);
                        // Update fileToDelete to the matching file for subsequent retries
                        fileToDelete = matchingFile;
                    } else if (matchingFiles.size() > 1) {
                        LOG.warn("Multiple matching in-progress files found for prefix {}: {}", filePrefix, matchingFiles);
                        return false;
                    } else {
                        LOG.warn("No matching in-progress file found for prefix: {}. File must have been deleted by some other process.", filePrefix);
                        return true;
                    }
                } catch (IOException e) {
                    LOG.warn("IOException while searching for matching in-progress file (attempt {}): {}", attempt + 1, file, e);
                }
            }
        }
        
        LOG.error("Failed to delete file after {} attempts: {}", maxRetries + 1, fileToDelete);
        return false;
    }

    protected Optional<Path> markInProgress(final Path file) {
        System.out.println("Mark In Progress Method Called for " + file.toString());
        try {
            String fileName = file.getName();
            String newFileName;
            Path targetDirectory;
            
            // Check if file is already in in-progress directory
            if(file.getParent().toUri().getPath().equals(inProgressDirPath.toString())) {
                // File is already in in-progress directory, replace UUID with a new one (stay in same directory)
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
                targetDirectory = inProgressDirPath;
            }
            
            Path newPath = new Path(targetDirectory, newFileName);
            if (fileSystem.rename(file, newPath)) {
                LOG.debug("Successfully marked file as in progress: {} -> {}", file.getName(), newFileName);
                return Optional.of(newPath);
            } else {
                LOG.warn("Failed to rename file for in-progress marking: {}", file);
                return Optional.empty();
            }
        } catch (IOException e) {
            LOG.error("IOException while marking file as in progress: {}", file, e);
            return Optional.empty();
        }
    }

    protected boolean isValidLogFile(Path path) {
        final String fileName = path.getName();
        return fileName.endsWith(".plog");
    }

    protected long getFileTimestamp(Path path) throws NumberFormatException {
        String[] parts = path.getName().split("_");
        return Long.parseLong(parts[0]);
    }

    protected Optional<String> getFileUUID(Path path) throws NumberFormatException {
        String[] parts = path.getName().split("_");
        if(parts.length < 3) {
            return Optional.empty();
        }
        return Optional.of(parts[parts.length-1].split("\\.")[0]);
    }

    /**
     * Extracts everything except the UUID (last part) from a file path.
     * For example, from "1704153600000_rs1_12345678-1234-1234-1234-123456789abc.plog"
     * returns "1704153600000_rs1"
     */
    protected String getFilePrefix(Path path) {
        String fileName = path.getName();
        String[] parts = fileName.split("_");
        if (parts.length < 2) {
            return fileName; // Return full filename if no underscore found
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

    public boolean markFailed(final Path file) {
        return true;
    }

    public FileSystem getFileSystem() {
        return this.fileSystem;
    }

    protected ReplicationShardDirectoryManager getReplicationShardDirectoryManager() {
        return this.replicationShardDirectoryManager;
    }

    protected String getHaGroupName() {
        return this.haGroupName;
    }

    protected Configuration getConf() {
        return this.conf;
    }

    protected Path getInProgressDirPath() {
        return inProgressDirPath;
    }

    private void createDirectoryIfNotExists(Path directoryPath) throws IOException {
        if (!fileSystem.exists(directoryPath)) {
            LOG.info("Creating directory {}", directoryPath);
            if (!fileSystem.mkdirs(directoryPath)) {
                throw new IOException("Failed to create directory: " + directoryPath);
            }
        }
    }
}
