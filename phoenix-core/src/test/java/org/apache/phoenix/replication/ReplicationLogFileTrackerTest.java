package org.apache.phoenix.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.ClassRule;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

public class ReplicationLogFileTrackerTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private TestableReplicationLogFileTracker tracker;
    private Configuration conf;
    private FileSystem localFs;
    private URI rootURI;
    private String haGroupName;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        rootURI = new Path(testFolder.getRoot().toString()).toUri();
        haGroupName = "testGroup";
        
        tracker = new TestableReplicationLogFileTracker(conf, haGroupName, localFs, rootURI);
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testInit() throws IOException {
        // Call init method
        tracker.init();

        // Verify in-progress path is set correctly
        Path actualInProgressPath = tracker.getInProgressDirPath();
        Path expectedInProgressPath = new Path(new Path(rootURI.getPath(), "in_progress"), haGroupName);
        assertNotNull("In-progress path should not be null", actualInProgressPath);
        assertEquals("In-progress path should be set correctly", expectedInProgressPath, actualInProgressPath);

        // Verify in-progress directory was created
        assertTrue("In-progress directory should exist", localFs.exists(expectedInProgressPath));
        assertTrue("In-progress directory should be a directory", localFs.isDirectory(expectedInProgressPath));

        // Verify ReplicationShardDirectoryManager was created
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        assertNotNull("ReplicationShardDirectoryManager should not be null", shardManager);
        
        // Verify the shard directory path is correct
        Path expectedShardDirectory = new Path(new Path(new Path(rootURI.getPath(), "in"), haGroupName), "shard");
        assertEquals("Shard directory should be set correctly", expectedShardDirectory, shardManager.getShardDirectoryPath());
    }

    @Test
    public void testInitWithDifferentHaGroupName() throws IOException {
        // Test with different HA group name
        String differentHaGroupName = "differentGroup";
        tracker = new TestableReplicationLogFileTracker(conf, differentHaGroupName, localFs, rootURI);
        
        // Call init method
        tracker.init();
        
        // Verify correct path was created
        Path expectedInProgressPath = new Path(new Path(rootURI.getPath(), "in_progress"), differentHaGroupName);
        assertTrue("In-progress directory should exist for different HA group", localFs.exists(expectedInProgressPath));
        assertTrue("In-progress directory should be a directory", localFs.isDirectory(expectedInProgressPath));
        
        // Verify shard directory uses correct HA group name
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        Path expectedShardDirectory = new Path(new Path(new Path(rootURI.getPath(), "in"), differentHaGroupName), "shard");
        assertEquals("Shard directory should use correct HA group name", expectedShardDirectory, shardManager.getShardDirectoryPath());
    }

    @Test
    public void testInitIsIdempotent() throws IOException {
        // Create the directory manually first
        Path expectedInProgressPath = new Path(new Path(rootURI.getPath(), "in_progress"), haGroupName);
        localFs.mkdirs(expectedInProgressPath);
        assertTrue("Directory should exist before init", localFs.exists(expectedInProgressPath));
        
        // Get initial directory count
        Path rootPath = new Path(rootURI.getPath());
        int initialDirCount = countDirectories(localFs, rootPath);
        
        // Call init method multiple times
        tracker.init();
        tracker.init();
        tracker.init();
        
        // Verify directory still exists and is valid
        assertTrue("In-progress directory should still exist", localFs.exists(expectedInProgressPath));
        assertTrue("In-progress directory should still be a directory", localFs.isDirectory(expectedInProgressPath));
        
        // Verify no additional directories were created
        int finalDirCount = countDirectories(localFs, rootPath);
        assertEquals("No additional directories should be created", initialDirCount, finalDirCount);
        
        // Verify ReplicationShardDirectoryManager is still valid
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        assertNotNull("ReplicationShardDirectoryManager should not be null after multiple init calls", shardManager);
        Path expectedShardDirectory = new Path(new Path(new Path(rootURI.getPath(), "in"), haGroupName), "shard");
        assertEquals("Shard directory should use correct HA group name", expectedShardDirectory, shardManager.getShardDirectoryPath());
    }

    @Test
    public void testGetNewFilesForRound() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a replication round (60 seconds duration)
        long roundStartTime = 1704153600000L; // 2024-01-02 00:00:00
        long roundEndTime = roundStartTime + TimeUnit.MINUTES.toMillis(1); // 60 seconds later
        ReplicationRound targetRound = new ReplicationRound(roundStartTime, roundEndTime);
        
        // Get the shard directory for this round
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        Path shardDirectory = shardManager.getShardDirectory(targetRound);
        
        // Create the shard directory
        localFs.mkdirs(shardDirectory);
        
        // Create files in the target round's shard directory
        Path fileInTargetRound1 = new Path(shardDirectory, "1704153600000_rs1.plog"); // Start of round
        Path fileInTargetRound2 = new Path(shardDirectory, "1704153630000_rs2.plog"); // Middle of round
        Path fileInTargetRound3 = new Path(shardDirectory, "1704153659000_rs3.plog"); // End of round
        
        // Create files in other rounds (same shard)
        Path fileInOtherRound1 = new Path(shardDirectory, "1704161280000_rs4.plog"); // future round for same shard
        Path fileInOtherRound2 = new Path(shardDirectory, "1704161310000_rs5.plog"); // Previous round
        
        // Create files in other shards
        Path fileInOtherShard1 = new Path(shardDirectory.getParent(), "001/1704153600000_rs6.plog");
        Path fileInOtherShard2 = new Path(shardDirectory.getParent(), "002/1704153600000_rs7.plog");
        
        // Create all directories and files
        localFs.mkdirs(fileInOtherShard1.getParent());
        localFs.mkdirs(fileInOtherShard2.getParent());
        
        // Create empty files
        localFs.create(fileInTargetRound1, true).close();
        localFs.create(fileInTargetRound2, true).close();
        localFs.create(fileInTargetRound3, true).close();
        localFs.create(fileInOtherRound1, true).close();
        localFs.create(fileInOtherRound2, true).close();
        localFs.create(fileInOtherShard1, true).close();
        localFs.create(fileInOtherShard2, true).close();
        
        // Call getNewFilesForRound
        List<Path> result = tracker.getNewFilesForRound(targetRound);

        Set<String> expectedPaths = new HashSet<>();
        expectedPaths.add(fileInTargetRound1.toString());
        expectedPaths.add(fileInTargetRound2.toString());
        expectedPaths.add(fileInTargetRound3.toString());
        
        // Create actual set of paths
        Set<String> actualPaths = result.stream().map(path -> path.toUri().getPath()).collect(Collectors.toSet());
        
        assertEquals("Should return exactly 3 files from target round", expectedPaths.size(), actualPaths.size());
        assertEquals("File paths do not match", expectedPaths, actualPaths);
    }

    @Test
    public void testGetNewFilesForRoundForNoFilesExist() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a replication round
        long roundStartTime = 1704153600000L;
        long roundEndTime = roundStartTime + TimeUnit.MINUTES.toMillis(1);
        ReplicationRound targetRound = new ReplicationRound(roundStartTime, roundEndTime);
        
        // Get the shard directory for this round
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        Path shardDirectory = shardManager.getShardDirectory(targetRound);
        
        // Create the shard directory but leave it empty
        localFs.mkdirs(shardDirectory);
        
        // Call getNewFilesForRound
        List<Path> result = tracker.getNewFilesForRound(targetRound);
        
        // Verify empty list is returned
        assertTrue("Should return empty list for empty directory", result.isEmpty());
    }

    @Test
    public void testGetNewFilesForRoundForNonExistentShardDirectory() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a replication round
        long roundStartTime = 1704153600000L;
        long roundEndTime = roundStartTime + TimeUnit.MINUTES.toMillis(1);
        ReplicationRound targetRound = new ReplicationRound(roundStartTime, roundEndTime);
        
        // Get the shard directory for this round
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        Path shardDirectory = shardManager.getShardDirectory(targetRound);
        
        // Assert that shard directory does not exist
        assertFalse("Shard directory should not exist", localFs.exists(shardDirectory));
        
        // Call getNewFilesForRound
        List<Path> result = tracker.getNewFilesForRound(targetRound);
        
        // Verify empty list is returned
        assertTrue("Should return empty list for non-existent directory", result.isEmpty());
    }

    @Test
    public void testGetNewFilesForRoundWithInvalidFiles() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a replication round
        long roundStartTime = 1704153600000L;
        long roundEndTime = roundStartTime + TimeUnit.MINUTES.toMillis(1);
        ReplicationRound targetRound = new ReplicationRound(roundStartTime, roundEndTime);
        
        // Get the shard directory for this round
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        Path shardDirectory = shardManager.getShardDirectory(targetRound);
        
        // Create the shard directory
        localFs.mkdirs(shardDirectory);
        
        // Create valid files
        Path validFile1 = new Path(shardDirectory, "1704153600000_rs1.plog");
        Path validFile2 = new Path(shardDirectory, "1704153630000_rs2.plog");
        
        // Create invalid files (wrong extension, invalid timestamp format)
        Path invalidFile1 = new Path(shardDirectory, "1704153600000_rs1.txt");
        Path invalidFile2 = new Path(shardDirectory, "invalid_timestamp_rs2.plog");
        Path invalidFile3 = new Path(shardDirectory, "1704153600000_rs3.log");
        
        // Create all files
        localFs.create(validFile1, true).close();
        localFs.create(validFile2, true).close();
        localFs.create(invalidFile1, true).close();
        localFs.create(invalidFile2, true).close();
        localFs.create(invalidFile3, true).close();
        
        // Call getNewFilesForRound
        List<Path> result = tracker.getNewFilesForRound(targetRound);
        
        // Prepare expected set of valid file paths
        Set<String> expectedPaths = new HashSet<>();
        expectedPaths.add(validFile1.toString());
        expectedPaths.add(validFile2.toString());
        
        // Create actual set of paths
        Set<String> actualPaths = result.stream().map(path -> path.toUri().getPath()).collect(Collectors.toSet());
        
        // Verify only valid files are returned
        assertEquals("Should return exactly 2 valid files", expectedPaths.size(), actualPaths.size());
        assertEquals("File paths do not match", expectedPaths, actualPaths);
    }

    @Test
    public void testGetInProgressFiles() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Get the in-progress directory path
        Path inProgressDir = tracker.getInProgressDirPath();
        
        // Create valid files in in-progress directory
        Path validFile1 = new Path(inProgressDir, "1704153600000_rs1.plog");
        Path validFile2 = new Path(inProgressDir, "1704153630000_rs2.plog");
        Path validFile3 = new Path(inProgressDir, "1704153659000_rs3.plog");
        
        // Create invalid files (wrong extension, invalid format)
        Path invalidFile1 = new Path(inProgressDir, "1704153600000_rs1.txt");
        Path invalidFile3 = new Path(inProgressDir, "1704153600000_rs3.log");
        
        // Create all files
        localFs.create(validFile1, true).close();
        localFs.create(validFile2, true).close();
        localFs.create(validFile3, true).close();
        localFs.create(invalidFile1, true).close();
        localFs.create(invalidFile3, true).close();
        
        // Call getInProgressFiles
        List<Path> result = tracker.getInProgressFiles();
        
        // Prepare expected set of valid file paths
        Set<String> expectedPaths = new HashSet<>();
        expectedPaths.add(validFile1.toString());
        expectedPaths.add(validFile2.toString());
        expectedPaths.add(validFile3.toString());
        
        // Create actual set of paths
        Set<String> actualPaths = result.stream().map(path -> path.toUri().getPath()).collect(Collectors.toSet());
        
        // Verify only valid files are returned
//        assertEquals("Should return exactly 3 valid files", expectedPaths.size(), actualPaths.size());
        assertEquals("File paths do not match", expectedPaths, actualPaths);
    }

    @Test
    public void testGetInProgressFilesForEmptyDirectory() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Get the in-progress directory path
        Path inProgressDir = tracker.getInProgressDirPath();
        
        // Ensure directory exists but is empty
        assertTrue("In-progress directory should exist", localFs.exists(inProgressDir));
        assertTrue("In-progress directory should be a directory", localFs.isDirectory(inProgressDir));
        
        // Call getInProgressFiles
        List<Path> result = tracker.getInProgressFiles();
        
        // Verify empty list is returned
        assertTrue("Should return empty list for empty directory", result.isEmpty());
    }

    @Test
    public void testGetInProgressFilesForNonExistentDirectory() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Get the in-progress directory path
        Path inProgressDir = tracker.getInProgressDirPath();
        
        // Delete the in-progress directory to make it non-existent
        localFs.delete(inProgressDir, true);
        assertFalse("In-progress directory should not exist", localFs.exists(inProgressDir));
        
        // Call getInProgressFiles
        List<Path> result = tracker.getInProgressFiles();
        
        // Verify empty list is returned
        assertTrue("Should return empty list for non-existent directory", result.isEmpty());
    }

    @Test
    public void testGetNewFiles() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Get all shard paths
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        List<Path> allShardPaths = shardManager.getAllShardPaths();
        
        // Create files in multiple shards
        Path shard0Path = allShardPaths.get(0);
        Path shard1Path = allShardPaths.get(1);
        Path shard2Path = allShardPaths.get(2);
        
        // Create valid files in shard 0
        Path validFile1 = new Path(shard0Path, "1704153600000_rs1.plog");
        Path validFile2 = new Path(shard0Path, "1704153630000_rs2.plog");
        
        // Create valid files in shard 1
        Path validFile3 = new Path(shard1Path, "1704153660000_rs3.plog");
        Path validFile4 = new Path(shard1Path, "1704153690000_rs4.plog");
        
        // Create valid files in shard 2
        Path validFile5 = new Path(shard2Path, "1704153720000_rs5.plog");
        
        // Create invalid files in shards
        Path invalidFile1 = new Path(shard0Path, "1704153600000_rs1.txt");
        Path invalidFile3 = new Path(shard2Path, "1704153720000_rs3.log");
        
        // Create all directories and files
        localFs.mkdirs(shard0Path);
        localFs.mkdirs(shard1Path);
        localFs.mkdirs(shard2Path);
        
        // Create valid files
        localFs.create(validFile1, true).close();
        localFs.create(validFile2, true).close();
        localFs.create(validFile3, true).close();
        localFs.create(validFile4, true).close();
        localFs.create(validFile5, true).close();
        
        // Create invalid files
        localFs.create(invalidFile1, true).close();
        localFs.create(invalidFile3, true).close();
        
        // Call getNewFiles
        List<Path> result = tracker.getNewFiles();
        
        // Prepare expected set of valid file paths
        Set<String> expectedPaths = new HashSet<>();
        expectedPaths.add(validFile1.toString());
        expectedPaths.add(validFile2.toString());
        expectedPaths.add(validFile3.toString());
        expectedPaths.add(validFile4.toString());
        expectedPaths.add(validFile5.toString());
        
        // Create actual set of paths
        Set<String> actualPaths = result.stream().map(path -> path.toUri().getPath()).collect(Collectors.toSet());
        
        // Verify all valid files from all shards are returned
        assertEquals("Should return exactly 5 valid files from all shards", expectedPaths.size(), actualPaths.size());
        assertEquals("File paths do not match", expectedPaths, actualPaths);
    }

    @Test
    public void testGetNewFilesForEmptyShards() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Get all shard paths
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        List<Path> allShardPaths = shardManager.getAllShardPaths();
        
        // Create directories but leave them empty
        for (Path shardPath : allShardPaths) {
            localFs.mkdirs(shardPath);
        }
        
        // Call getNewFiles
        List<Path> result = tracker.getNewFiles();
        
        // Verify empty list is returned
        assertTrue("Should return empty list for empty shards", result.isEmpty());
    }

    @Test
    public void testGetNewFilesForNonExistentShards() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Get all shard paths
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        List<Path> allShardPaths = shardManager.getAllShardPaths();
        
        // Assert that no shard directories exist
        for (Path shardPath : allShardPaths) {
            assertFalse("Shard directory should not exist: " + shardPath, localFs.exists(shardPath));
        }
        
        // Call getNewFiles
        List<Path> result = tracker.getNewFiles();
        
        // Verify empty list is returned
                assertTrue("Should return empty list for non-existent shards", result.isEmpty());
    }

    @Test
    public void testMarkInProgressForNewFile() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a file in a shard directory (without UUID)
        ReplicationShardDirectoryManager shardManager = tracker.getReplicationShardDirectoryManager();
        List<Path> allShardPaths = shardManager.getAllShardPaths();
        Path shardPath = allShardPaths.get(0);
        localFs.mkdirs(shardPath);
        
        // Create original file without UUID
        Path originalFile = new Path(shardPath, "1704153600000_rs1.plog");
        localFs.create(originalFile, true).close();
        
        // Verify original file exists
        assertTrue("Original file should exist", localFs.exists(originalFile));
        
        // Call markInProgress
        Optional<Path> result = tracker.markInProgress(originalFile);
        
        // Verify operation was successful
        assertTrue("markInProgress should be successful", result.isPresent());

        // TODO: Assert that new file has same prefix as original file and it's in in-progress directory
        
        // Verify original file no longer exists
        assertFalse("Original file should no longer exist", localFs.exists(originalFile));
        
        // Verify file was moved to in-progress directory with UUID
        Path inProgressDir = tracker.getInProgressDirPath();
        FileStatus[] files = localFs.listStatus(inProgressDir);
        assertEquals("Should have exactly one file in in-progress directory", 1, files.length);
        
        // Verify the new file has UUID format
        String newFileName = files[0].getPath().getName();
        assertTrue("New file should have UUID suffix", newFileName.matches("1704153600000_rs1_[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\\.plog"));
    }

    @Test
    public void testMarkInProgressForAlreadyInProgressFile() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a file in in-progress directory with existing UUID
        Path inProgressDir = tracker.getInProgressDirPath();
        String existingUUID = "12345678-1234-1234-1234-123456789abc";
        Path originalFile = new Path(inProgressDir, "1704153600000_rs1_" + existingUUID + ".plog");
        localFs.create(originalFile, true).close();
        
        // Verify original file exists
        assertTrue("Original file should exist", localFs.exists(originalFile));
        
        // Call markInProgress
        Optional<Path> result = tracker.markInProgress(originalFile);
        
        // Verify operation was successful
        assertTrue("markInProgress should be successful", result.isPresent());

        // TODO: Assert that new file has same prefix as original file and it's in in-progress directory
        
        // Verify original file no longer exists
        assertFalse("Original file should no longer exist", localFs.exists(originalFile));
        
        // Verify new file exists in same directory with new UUID
        FileStatus[] files = localFs.listStatus(inProgressDir);
        assertEquals("Should have exactly one file in in-progress directory", 1, files.length);

        System.out.println("Find files: " + files.length);
        for(FileStatus fileStatus : files) {
            System.out.println(fileStatus.getPath());
        }
        
        // Verify the new file has different UUID
        String newFileName = files[0].getPath().getName();
        assertTrue("New file should have UUID suffix", newFileName.matches("1704153600000_rs1_[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\\.plog"));
        assertFalse("New file should have different UUID", newFileName.contains(existingUUID));
    }

    @Test
    public void testMarkInProgressForNonExistentFile() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create a non-existent file path
        Path nonExistentFile = new Path(tracker.getInProgressDirPath(), "non_existent_file.plog");
        
        // Verify file doesn't exist
        assertFalse("File should not exist", localFs.exists(nonExistentFile));
        
        // Call markInProgress
        Optional<Path> result = tracker.markInProgress(nonExistentFile);
        
        // Verify operation failed
        assertFalse("markInProgress should return false for non-existent file", result.isPresent());
    }

    @Test
    public void testMarkCompletedSuccessfulDeletion() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create original file in shard directory
        Path originalFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789xyz.plog");
        localFs.create(originalFile, true).close();
        
        // Verify original file exists
        assertTrue("Original file should exist", localFs.exists(originalFile));
        
        // Call markCompleted on the original file
        boolean result = tracker.markCompleted(originalFile);
        
        // Verify operation was successful
        assertTrue("markCompleted should return true for successful deletion", result);
        
        // Verify original file no longer exists
        assertFalse("Original file should no longer exist", localFs.exists(originalFile));
    }

    @Test
    public void testMarkCompletedIntermittentDeletionFailure() throws IOException {
        // Initialize tracker
        tracker.init();

        // Create original file in in-progress directory with UUID
        Path originalFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789pqr.plog");
        localFs.create(originalFile, true).close();

        // Verify original file exists
        assertTrue("Original file should exist", localFs.exists(originalFile));

        // Create a spy on the real FileSystem that throws IOException for first 2 attempts, then succeeds
        FileSystem mockFs = spy(localFs);
        doThrow(new IOException("Simulated IO error"))
                .doThrow(new IOException("Simulated IO error"))
                .doReturn(true)
                .when(mockFs).delete(Mockito.argThat(path -> path.getName().equals(originalFile.getName())), Mockito.eq(false));

        // Replace the tracker's filesystem with our mock
        TestableReplicationLogFileTracker mockTracker = new TestableReplicationLogFileTracker(conf, haGroupName, mockFs, rootURI);
        mockTracker.init();

        // Call markCompleted on the original file
        boolean result = mockTracker.markCompleted(originalFile);

        // Verify operation was successful after retries
        assertTrue("markCompleted should return true after successful retry", result);

        // Verify that delete was called exactly 3 times (2 failures + 1 success)
        verify(mockFs, times(3)).delete(Mockito.argThat(path -> path.getName().equals(originalFile.getName())), Mockito.eq(false));
    }

    @Test
    public void testMarkCompletedForPersistentDeletionFailure() throws IOException {

        // Create a spy on the real FileSystem that throws IOException for all attempts
        FileSystem mockFs = spy(localFs);

        // Replace the tracker's filesystem with our spy
        TestableReplicationLogFileTracker mockTracker = new TestableReplicationLogFileTracker(conf, haGroupName, mockFs, rootURI);
        mockTracker.init();

        // Create original file in in-progress directory with UUID
        Path originalFile = new Path(mockTracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789def.plog");

        // Set up the mock BEFORE creating the file
        // Mock delete for any file with the same name to throw IOException
        doThrow(new IOException("Simulated IO error"))
                .doThrow(new IOException("Simulated IO error"))
                .doThrow(new IOException("Simulated IO error"))
                .doThrow(new IOException("Simulated IO error"))
                .when(mockFs).delete(Mockito.argThat(path -> path.getName().equals(originalFile.getName())), Mockito.eq(false));

        // Create the file using the spy filesystem
        mockFs.create(originalFile, true).close();

        // Verify original file exists
        assertTrue("Original file should exist", mockFs.exists(originalFile));
        
        // Call markCompleted on the original file
        boolean result = mockTracker.markCompleted(originalFile);

        // Verify operation failed after all retries
        assertFalse("markCompleted should return false after all retries fail", result);

        // Verify that delete was called exactly 4 times (all attempts failed)
        // Default retries is 3, so total attempts = 1 + 3 = 4
        verify(mockFs, times(4)).delete(Mockito.any(), Mockito.eq(false));
    }

    @Test
    public void testMarkCompletedWhenFileDeletedByOtherProcessAlready() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Create original file in in-progress directory with UUID
        Path fileToBeDeleted = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789jkl.plog");
        localFs.create(fileToBeDeleted, true).close();
        
        // Create in-progress file with different prefix
        String uuid = "12345678-1234-1234-1234-123456789abc";
        Path anotherInProgressFile = new Path(tracker.getInProgressDirPath(), "1704153600001_rs2_" + uuid + ".plog");
        localFs.create(anotherInProgressFile, true).close();

        // Delete the first file
        localFs.delete(fileToBeDeleted, true);

        // Assert first file is deleted and second file should exist
        assertFalse("Original file to be deleted should not exist", localFs.exists(fileToBeDeleted));
        assertTrue("Second file should exist", localFs.exists(anotherInProgressFile));
        
        // Call markCompleted on the deleted file
        // Since the file is already deleted, it should search for other files with same prefix
        // but find none, so it should return true (assuming file was deleted by another process)
        boolean result = tracker.markCompleted(fileToBeDeleted);
        
        // Verify operation was successful (no matching files found)
        assertTrue("markCompleted should return true when file is already deleted and no matching files found", result);
        
        // Verify the other in-progress file still exists (was not affected)
        assertTrue("Other in-progress file should still exist", localFs.exists(anotherInProgressFile));
    }

    @Test
    public void testMarkCompletedWithMultipleMatchingFiles() throws IOException {
        // Initialize tracker
        tracker.init();

        // Create original file in in-progress directory with UUID
        Path originalFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789ghi.plog");
        localFs.create(originalFile, true).close();

        // Create multiple in-progress files with same prefix
        Path inProgressDir = tracker.getInProgressDirPath();
        String uuid1 = "12345678-1234-1234-1234-123456789abc";
        String uuid2 = "87654321-4321-4321-4321-cba987654321";
        Path inProgressFile1 = new Path(inProgressDir, "1704153600000_rs1_" + uuid1 + ".plog");
        Path inProgressFile2 = new Path(inProgressDir, "1704153600000_rs1_" + uuid2 + ".plog");
        localFs.create(inProgressFile1, true).close();
        localFs.create(inProgressFile2, true).close();

        // Delete the first file
        localFs.delete(originalFile, true);

        // Verify files exist
        assertFalse("Original file should not exist", localFs.exists(originalFile));
        assertTrue("In-progress file 1 should exist", localFs.exists(inProgressFile1));
        assertTrue("In-progress file 2 should exist", localFs.exists(inProgressFile2));

        // Call markCompleted on the original file
        // Should return false due to multiple matching files during retries
        boolean result = tracker.markCompleted(originalFile);

        // Verify operation failed
        assertFalse("markCompleted should return false for multiple matching files", result);

        // Verify the files still exist
        assertTrue("In-progress file 1 should still exist", localFs.exists(inProgressFile1));
        assertTrue("In-progress file 2 should still exist", localFs.exists(inProgressFile2));
    }

    @Test
    public void testGetFileTimestamp() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Test with new file (without UUID)
        Path newFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1.plog");
        long newFileTimestamp = tracker.getFileTimestamp(newFile);
        assertEquals("New file timestamp should be extracted correctly", 1704153600000L, newFileTimestamp);
        
        // Test with in-progress file (with UUID)
        Path inProgressFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789abc.plog");
        long inProgressFileTimestamp = tracker.getFileTimestamp(inProgressFile);
        assertEquals("In-progress file timestamp should be extracted correctly", 1704153600000L, inProgressFileTimestamp);
        
        // Verify both timestamps are the same
        assertEquals("Both files should have the same timestamp", newFileTimestamp, inProgressFileTimestamp);
        
        // Test with different timestamp
        Path anotherValidFile = new Path(tracker.getInProgressDirPath(), "1704161280000_rs2_87654321-4321-4321-4321-cba987654321.plog");
        long anotherTimestamp = tracker.getFileTimestamp(anotherValidFile);
        assertEquals("Should extract correct timestamp", 1704161280000L, anotherTimestamp);
    }

    @Test(expected = NumberFormatException.class)
    public void testGetFileTimestampWithInvalidFormat() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Test file with invalid timestamp format (non-numeric)
        Path invalidFile = new Path(tracker.getInProgressDirPath(), "invalid_timestamp_rs1_12345678-1234-1234-1234-123456789abc.plog");
        tracker.getFileTimestamp(invalidFile);
        // Should throw NumberFormatException
    }

    @Test(expected = NumberFormatException.class)
    public void testGetFileTimestampWithMissingTimestamp() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // Test file with missing timestamp part
        Path invalidFile = new Path(tracker.getInProgressDirPath(), "rs1_12345678-1234-1234-1234-123456789abc.plog");
        tracker.getFileTimestamp(invalidFile);
    }

    @Test
    public void testIsValidLogFile() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // 1. New File (valid) - without UUID, with .plog extension
        Path newFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1.plog");
        assertTrue("New file with .plog extension should be valid", tracker.isValidLogFile(newFile));
        
        // 2. InProgressFile (valid) - with UUID, with .plog extension
        Path inProgressFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789abc.plog");
        assertTrue("In-progress file with .plog extension should be valid", tracker.isValidLogFile(inProgressFile));
        
        // 3. Valid file (invalid extension) - without UUID, without .plog extension
        Path newFileInvalidExt = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1.txt");
        assertFalse("New file without .plog extension should be invalid", tracker.isValidLogFile(newFileInvalidExt));
        
        // 4. InProgress (invalid extension) - with UUID, without .plog extension
        Path inProgressFileInvalidExt = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789abc.txt");
        assertFalse("In-progress file without .plog extension should be invalid", tracker.isValidLogFile(inProgressFileInvalidExt));
    }

    @Test
    public void testGetFileUUID() throws IOException {
        // Initialize tracker
        tracker.init();
        
        // 1. For new File - without UUID
        Path newFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1.plog");
        Optional<String> newFileUUID = tracker.getFileUUID(newFile);
        assertFalse("New file without UUID should return empty Optional", newFileUUID.isPresent());
        
        // 2. For in-progress file - with UUID
        Path inProgressFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_12345678-1234-1234-1234-123456789abc.plog");
        Optional<String> inProgressFileUUID = tracker.getFileUUID(inProgressFile);
        assertTrue("In-progress file with UUID should return present Optional", inProgressFileUUID.isPresent());
        assertEquals("In-progress file UUID should be extracted correctly", "12345678-1234-1234-1234-123456789abc", inProgressFileUUID.get());
        
        // Test with different UUID
        Path anotherInProgressFile = new Path(tracker.getInProgressDirPath(), "1704153600000_rs1_87654321-4321-4321-4321-cba987654321.plog");
        Optional<String> anotherUUID = tracker.getFileUUID(anotherInProgressFile);
        assertTrue("Another in-progress file with UUID should return present Optional", anotherUUID.isPresent());
        assertEquals("Another in-progress file UUID should be extracted correctly", "87654321-4321-4321-4321-cba987654321", anotherUUID.get());
    }

    private int countDirectories(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            return 0;
        }
        int count = 0;
        if (fs.isDirectory(path)) {
            count = 1; // Count this directory
            try {
                for (org.apache.hadoop.fs.FileStatus status : fs.listStatus(path)) {
                    if (status.isDirectory()) {
                        count += countDirectories(fs, status.getPath());
                    }
                }
            } catch (IOException e) {
                // Ignore listing errors for test purposes
            }
        }
        return count;
    }

    private static class TestableReplicationLogFileTracker extends ReplicationLogFileTracker {
        public TestableReplicationLogFileTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) {
            super(conf, haGroupName, fileSystem, rootURI);
        }

        @Override
        protected String getNewLogSubDirectoryName() {
            return "in";
        }
    }
}
