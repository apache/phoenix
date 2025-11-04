/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationShardDirectoryManagerTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private URI standbyUri;
    private ReplicationShardDirectoryManager manager;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        standbyUri = new Path(testFolder.toString()).toUri();
        conf.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());

        // Create manager with default configuration
        Path rootPath = new Path(testFolder.getRoot().getAbsolutePath());
        manager = new ReplicationShardDirectoryManager(conf, rootPath);
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testGetShardDirectoryWithSpecificConditions() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Calculate expected base path
        String expectedBasePath = testFolder.getRoot().getAbsolutePath() + "/shard";

        // 1. Timestamp at start of min (00:00:00)
        long timestampStartOfMin = dayStart; // 00:00:00
        Path shardPath1 = manager.getShardDirectory(timestampStartOfMin);
        String expectedPath1 = expectedBasePath + "/000";
        assertEquals("Timestamp at start of min should map to shard 000",
                   expectedPath1, shardPath1.toString());

        // 2. Timestamp at mid of min (00:00:30)
        long timestampMidOfMin = dayStart + (30 * 1000L); // 00:00:30
        Path shardPath2 = manager.getShardDirectory(timestampMidOfMin);
        String expectedPath2 = expectedBasePath + "/000"; // Still in first 60-second round
        assertEquals("Timestamp at mid of min should map to shard 000",
                   expectedPath2, shardPath2.toString());

        // 3. Timestamp at just end of min (00:00:59)
        long timestampEndOfMin = dayStart + (59 * 1000L); // 00:00:59
        Path shardPath3 = manager.getShardDirectory(timestampEndOfMin);
        String expectedPath3 = expectedBasePath + "/000"; // Still in first 60-second round
        assertEquals("Timestamp at end of min should map to shard 000",
                   expectedPath3, shardPath3.toString());

        // 4. Timestamp at which modulo logic is used (128th round = 128 minutes later)
        // 128 * 60 seconds = 7680 seconds
        long timestampModuloLogic = dayStart + (128 * 60 * 1000L); // 02:08:00
        Path shardPath4 = manager.getShardDirectory(timestampModuloLogic);
        String expectedPath4 = expectedBasePath + "/000"; // Should wrap around to shard 0
        assertEquals("Timestamp at 128th round should wrap to shard 000 due to modulo",
                   expectedPath4, shardPath4.toString());

        // Additional test: Second round (00:01:00)
        long timestampSecondRound = dayStart + (60 * 1000L); // 00:01:00
        Path shardPath5 = manager.getShardDirectory(timestampSecondRound);
        String expectedPath5 = expectedBasePath + "/001";
        assertEquals("Timestamp at second round should map to shard 001",
                   expectedPath5, shardPath5.toString());

        // Additional test: 127th round (should be shard 127)
        long timestamp127thRound = dayStart + (127 * 60 * 1000L); // 02:07:00
        Path shardPath6 = manager.getShardDirectory(timestamp127thRound);
        String expectedPath6 = expectedBasePath + "/127";
        assertEquals("Timestamp at 127th round should map to shard 127",
                   expectedPath6, shardPath6.toString());

        // Additional test: 129th round (should wrap to shard 1)
        long timestamp129thRound = dayStart + (129 * 60 * 1000L); // 02:09:00
        Path shardPath7 = manager.getShardDirectory(timestamp129thRound);
        String expectedPath7 = expectedBasePath + "/001";
        assertEquals("Timestamp at 129th round should wrap to shard 001",
                   expectedPath7, shardPath7.toString());
    }

    @Test
    public void testGetShardDirectoryForTwoDayWindow() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Calculate expected base path
        String expectedBasePath = testFolder.getRoot().getAbsolutePath() + "/shard";

        // Test for each minute in the 2-day window
        for (int day = 0; day < 2; day++) {
            for (int hour = 0; hour < 24; hour++) {
                for (int minute = 0; minute < 60; minute++) {
                    long minuteStart = dayStart + (day * 24 * 60 * 60 * 1000L) +
                                     (hour * 60 * 60 * 1000L) + (minute * 60 * 1000L);

                    // Calculate expected shard index based on the algorithm
                    // Convert to seconds since start of day
                    long secondsSinceEpoch = minuteStart / 1000L;
                    long secondsSinceStartOfDay = secondsSinceEpoch % TimeUnit.DAYS.toSeconds(1);
                    int shardIndex = (int) (secondsSinceStartOfDay / 60) % 128; // 60-second rounds, 128 shards

                    // 1. Test start of minute (XX:XX:00)
                    long timestampStartOfMin = minuteStart;
                    Path shardPathStart = manager.getShardDirectory(timestampStartOfMin);
                    String expectedPathStart = expectedBasePath + "/" + String.format("%03d", shardIndex);
                    assertEquals(String.format("Start of minute %02d:%02d:%02d (day %d) should map to shard %03d",
                                           hour, minute, 0, day + 1, shardIndex),
                               expectedPathStart, shardPathStart.toString());

                    // 2. Test mid of minute (random between 2-58 seconds)
                    int randomSeconds = 2 + (int)(Math.random() * 57); // Random between 2-58
                    long timestampMidOfMin = minuteStart + (randomSeconds * 1000L);
                    Path shardPathMid = manager.getShardDirectory(timestampMidOfMin);
                    String expectedPathMid = expectedBasePath + "/" + String.format("%03d", shardIndex);
                    assertEquals(String.format("Mid of minute %02d:%02d:%02d (day %d) should map to shard %03d",
                                           hour, minute, randomSeconds, day + 1, shardIndex),
                               expectedPathMid, shardPathMid.toString());

                    // 3. Test end of minute (XX:XX:59)
                    long timestampEndOfMin = minuteStart + (59 * 1000L);
                    Path shardPathEnd = manager.getShardDirectory(timestampEndOfMin);
                    String expectedPathEnd = expectedBasePath + "/" + String.format("%03d", shardIndex);
                    assertEquals(String.format("End of minute %02d:%02d:%02d (day %d) should map to shard %03d",
                                           hour, minute, 59, day + 1, shardIndex),
                               expectedPathEnd, shardPathEnd.toString());
                }
            }
        }
    }

    @Test
    public void testGetShardDirectoryWithReplicationRound() {
        // Create a spy of the manager to verify method calls
        ReplicationShardDirectoryManager spyManager = spy(manager);

        // Create a replication round
        long startTime = 1704110400000L; // 2024-01-01 12:00:00 UTC
        long endTime = startTime + (60 * 1000L); // 60 seconds later
        ReplicationRound round = new ReplicationRound(startTime, endTime);

        // Call the method that takes ReplicationRound
        spyManager.getShardDirectory(round);

        // Verify that it calls the timestamp version with the correct start time
        verify(spyManager).getShardDirectory(eq(startTime));
    }

    @Test
    public void testGetNearestRoundStartTimestamp() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Default configuration: 60-second rounds
        long roundDurationMs = 60 * 1000L; // 60 seconds in milliseconds

        // Test 1: Exact round start time
        long exactRoundStart = dayStart; // 00:00:00
        long result1 = manager.getNearestRoundStartTimestamp(exactRoundStart);
        assertEquals("Exact round start time should return itself", exactRoundStart, result1);

        // Test 2: Middle of first round
        long midFirstRound = dayStart + (30 * 1000L); // 00:00:30
        long result2 = manager.getNearestRoundStartTimestamp(midFirstRound);
        assertEquals("Middle of first round should round down to start", dayStart, result2);

        // Test 3: End of first round
        long endFirstRound = dayStart + (59 * 1000L); // 00:00:59
        long result3 = manager.getNearestRoundStartTimestamp(endFirstRound);
        assertEquals("End of first round should round down to start", dayStart, result3);

        // Test 4: Start of second round
        long startSecondRound = dayStart + (60 * 1000L); // 00:01:00
        long result4 = manager.getNearestRoundStartTimestamp(startSecondRound);
        assertEquals("Start of second round should return itself", startSecondRound, result4);

        // Test 5: Middle of second round
        long midSecondRound = dayStart + (90 * 1000L); // 00:01:30
        long result5 = manager.getNearestRoundStartTimestamp(midSecondRound);
        assertEquals("Middle of second round should round down to start", startSecondRound, result5);

        // Test 6: End of second round
        long endSecondRound = dayStart + (119 * 1000L); // 00:01:59
        long result6 = manager.getNearestRoundStartTimestamp(endSecondRound);
        assertEquals("End of second round should round down to start", startSecondRound, result6);

        // Test 7: Multiple rounds later
        long multipleRoundsLater = dayStart + (300 * 1000L); // 00:05:00 (5 minutes)
        long expectedRoundStart = dayStart + (300 * 1000L); // Should be exac
        long result7 = manager.getNearestRoundStartTimestamp(multipleRoundsLater);
        assertEquals("Multiple rounds later should return exact round start", expectedRoundStart, result7);

        // Test 8: Just before a round boundary
        long justBeforeRound = dayStart + (299 * 1000L); // 00:04:59
        long expectedBeforeRound = dayStart + (240 * 1000L); // 00:04:00
        long result8 = manager.getNearestRoundStartTimestamp(justBeforeRound);
        assertEquals("Just before round boundary should round down", expectedBeforeRound, result8);

        // Test 9: Just after a round boundary
        long justAfterRound = dayStart + (301 * 1000L); // 00:05:01
        long expectedAfterRound = dayStart + (300 * 1000L); // 00:05:00
        long result9 = manager.getNearestRoundStartTimestamp(justAfterRound);
        assertEquals("Just after round boundary should round down", expectedAfterRound, result9);

        // Test 11: Current time (should round down to nearest round)
        long currentTime = System.currentTimeMillis();
        long result11 = manager.getNearestRoundStartTimestamp(currentTime);
        Assert.assertTrue("Current time should round down to nearest round", result11 <= currentTime);
        assertEquals("Result should be a multiple of round duration", 0, (result11 % roundDurationMs));
    }

    @Test
    public void testGetNearestRoundStartTimestampWithCustomConfiguration() {
        // Test with custom round duration
        Configuration customConf = HBaseConfiguration.create();
        customConf.setInt(ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 30);

        Path rootPath = new Path(testFolder.getRoot().getAbsolutePath());
        ReplicationShardDirectoryManager customManager = new ReplicationShardDirectoryManager(customConf, rootPath);

        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Test with 30-second rounds
        long midRound = dayStart + (45 * 1000L); // 00:00:45
        long result = customManager.getNearestRoundStartTimestamp(midRound);
        long expected = dayStart + (30 * 1000L); // 00:00:30
        assertEquals("With 30-second rounds, should round down to 30-second boundary", expected, result);

        // Test exact 30-second boundary
        long exactBoundary = dayStart + (60 * 1000L); // 00:01:00
        long result2 = customManager.getNearestRoundStartTimestamp(exactBoundary);
        assertEquals("Exact 30-second boundary should return itself", exactBoundary, result2);
    }

    @Test
    public void testGetReplicationRoundFromStartTime() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Default configuration: 60-second rounds
        long roundDurationMs = 60 * 1000L; // 60 seconds in milliseconds

        // Test 1: Exact round start time
        long exactRoundStart = dayStart; // 00:00:00
        ReplicationRound result1 = manager.getReplicationRoundFromStartTime(exactRoundStart);
        assertEquals("Round start time should be validated and unchanged", exactRoundStart, result1.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   exactRoundStart + roundDurationMs, result1.getEndTime());

        // Test 2: Mid-round timestamp (should be rounded down)
        long midRoundTimestamp = dayStart + (30 * 1000L); // 00:00:30
        ReplicationRound result2 = manager.getReplicationRoundFromStartTime(midRoundTimestamp);
        long expectedStart2 = dayStart; // Should round down to 00:00:00
        assertEquals("Mid-round timestamp should round down to round start", expectedStart2, result2.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   expectedStart2 + roundDurationMs, result2.getEndTime());

        // Test 3: End of round timestamp (should round down)
        long endRoundTimestamp = dayStart + (59 * 1000L); // 00:00:59
        ReplicationRound result3 = manager.getReplicationRoundFromStartTime(endRoundTimestamp);
        long expectedStart3 = dayStart; // Should round down to 00:00:00
        assertEquals("End of round timestamp should round down to round start", expectedStart3, result3.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   expectedStart3 + roundDurationMs, result3.getEndTime());

        // Test 4: Start of second round
        long secondRoundStart = dayStart + (60 * 1000L); // 00:01:00
        ReplicationRound result4 = manager.getReplicationRoundFromStartTime(secondRoundStart);
        assertEquals("Second round start time should be validated and unchanged", secondRoundStart, result4.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   secondRoundStart + roundDurationMs, result4.getEndTime());

        // Test 5: Mid-second round timestamp
        long midSecondRound = dayStart + (90 * 1000L); // 00:01:30
        ReplicationRound result5 = manager.getReplicationRoundFromStartTime(midSecondRound);
        long expectedStart5 = dayStart + (60 * 1000L); // Should round down to 00:01:00
        assertEquals("Mid-second round timestamp should round down to round start", expectedStart5, result5.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   expectedStart5 + roundDurationMs, result5.getEndTime());

        // Test 6: Multiple rounds later
        long multipleRoundsLater = dayStart + (300 * 1000L); // 00:05:00
        ReplicationRound result6 = manager.getReplicationRoundFromStartTime(multipleRoundsLater);
        assertEquals("Multiple rounds later should be validated and unchanged", multipleRoundsLater, result6.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   multipleRoundsLater + roundDurationMs, result6.getEndTime());

        // Test 7: Just before round boundary
        long justBeforeRound = dayStart + (299 * 1000L); // 00:04:59
        ReplicationRound result7 = manager.getReplicationRoundFromStartTime(justBeforeRound);
        long expectedStart7 = dayStart + (240 * 1000L); // Should round down to 00:04:00
        assertEquals("Just before round boundary should round down", expectedStart7, result7.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   expectedStart7 + roundDurationMs, result7.getEndTime());

        // Test 8: Just after round boundary
        long justAfterRound = dayStart + (301 * 1000L); // 00:05:01
        ReplicationRound result8 = manager.getReplicationRoundFromStartTime(justAfterRound);
        long expectedStart8 = dayStart + (300 * 1000L); // Should round down to 00:05:00
        assertEquals("Just after round boundary should round down", expectedStart8, result8.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   expectedStart8 + roundDurationMs, result8.getEndTime());

        // Test 10: Current time (should round down to nearest round)
        long currentTime = System.currentTimeMillis();
        ReplicationRound result10 = manager.getReplicationRoundFromStartTime(currentTime);
        Assert.assertTrue("Current time should round down to nearest round", result10.getStartTime() <= currentTime);
        assertEquals("Round start should be a multiple of round duration", 0, (result10.getStartTime() % roundDurationMs));
        assertEquals("Round end time should be start time + round duration",
                   result10.getStartTime() + roundDurationMs, result10.getEndTime());
    }

    @Test
    public void testGetReplicationRoundFromStartTimeWithCustomConfiguration() {
        // Test with custom round duration
        Configuration customConf = HBaseConfiguration.create();
        customConf.setInt(ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 30);

        Path rootPath = new Path(testFolder.getRoot().getAbsolutePath());
        ReplicationShardDirectoryManager customManager = new ReplicationShardDirectoryManager(customConf, rootPath);

        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC
        long roundDurationMs = 30 * 1000L; // 30 seconds in milliseconds

        // Test with 30-second rounds
        long midRound = dayStart + (45 * 1000L); // 00:00:45
        ReplicationRound result = customManager.getReplicationRoundFromStartTime(midRound);
        long expectedStart = dayStart + (30 * 1000L); // 00:00:30
        assertEquals("With 30-second rounds, should round down to 30-second boundary", expectedStart, result.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   expectedStart + roundDurationMs, result.getEndTime());

        // Test exact 30-second boundary
        long exactBoundary = dayStart + (60 * 1000L); // 00:01:00
        ReplicationRound result2 = customManager.getReplicationRoundFromStartTime(exactBoundary);
        assertEquals("Exact 30-second boundary should be validated and unchanged", exactBoundary, result2.getStartTime());
        assertEquals("Round end time should be start time + round duration",
                   exactBoundary + roundDurationMs, result2.getEndTime());
    }

    @Test
    public void testGetReplicationRoundFromStartTimeConsistency() {
        // Test that the same input always produces the same outpu
        long timestamp = 1704110400000L; // 2024-01-01 12:00:00 UTC

        ReplicationRound result1 = manager.getReplicationRoundFromStartTime(timestamp);
        ReplicationRound result2 = manager.getReplicationRoundFromStartTime(timestamp);
        ReplicationRound result3 = manager.getReplicationRoundFromStartTime(timestamp);

        assertEquals("Same input should produce same start time", result1.getStartTime(), result2.getStartTime());
        assertEquals("Same input should produce same end time", result1.getEndTime(), result2.getEndTime());
        assertEquals("Same input should produce same start time", result1.getStartTime(), result3.getStartTime());
        assertEquals("Same input should produce same end time", result1.getEndTime(), result3.getEndTime());

        // Test that round duration is consisten
        long roundDuration1 = result1.getEndTime() - result1.getStartTime();
        assertEquals("Round duration should be 60 seconds", 60 * 1000L, roundDuration1);
    }

    @Test
    public void testGetReplicationRoundFromEndTime() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Default configuration: 60-second rounds
        long roundDurationMs = 60 * 1000L; // 60 seconds in milliseconds

        // Test 1: Exact round end time
        long exactRoundEnd = dayStart + roundDurationMs; // 00:01:00
        ReplicationRound result1 = manager.getReplicationRoundFromEndTime(exactRoundEnd);
        assertEquals("Round end time should be validated and unchanged", exactRoundEnd, result1.getEndTime());
        assertEquals("Round start time should be end time - round duration",
                   exactRoundEnd - roundDurationMs, result1.getStartTime());

        // Test 2: Mid-round timestamp (should be rounded down to round start, then calculate end)
        long midRoundTimestamp = dayStart + (30 * 1000L); // 00:00:30
        ReplicationRound result2 = manager.getReplicationRoundFromEndTime(midRoundTimestamp);
        long expectedEnd2 = dayStart; // Should round down to 00:00:00 (round start)
        long expectedStart2 = expectedEnd2 - roundDurationMs; // Should be -60 seconds (edge case)
        assertEquals("Mid-round timestamp should round down to round start", expectedEnd2, result2.getEndTime());
        assertEquals("Round start time should be end time - round duration", expectedStart2, result2.getStartTime());

        // Test 3: End of first round
        long endFirstRound = dayStart + (59 * 1000L); // 00:00:59
        ReplicationRound result3 = manager.getReplicationRoundFromEndTime(endFirstRound);
        long expectedEnd3 = dayStart; // Should round down to 00:00:00
        long expectedStart3 = expectedEnd3 - roundDurationMs; // Should be -60 seconds
        assertEquals("End of first round should round down to round start", expectedEnd3, result3.getEndTime());
        assertEquals("Round start time should be end time - round duration", expectedStart3, result3.getStartTime());

        // Test 4: Start of second round (exact boundary)
        long startSecondRound = dayStart + (60 * 1000L); // 00:01:00
        ReplicationRound result4 = manager.getReplicationRoundFromEndTime(startSecondRound);
        assertEquals("Second round start time should be validated and unchanged", startSecondRound, result4.getEndTime());
        assertEquals("Round start time should be end time - round duration",
                   startSecondRound - roundDurationMs, result4.getStartTime());

        // Test 5: Mid-second round timestamp
        long midSecondRound = dayStart + (90 * 1000L); // 00:01:30
        ReplicationRound result5 = manager.getReplicationRoundFromEndTime(midSecondRound);
        long expectedEnd5 = dayStart + (60 * 1000L); // Should round down to 00:01:00
        long expectedStart5 = expectedEnd5 - roundDurationMs; // Should be 00:00:00
        assertEquals("Mid-second round timestamp should round down to round start", expectedEnd5, result5.getEndTime());
        assertEquals("Round start time should be end time - round duration", expectedStart5, result5.getStartTime());

        // Test 6: Multiple rounds later
        long multipleRoundsLater = dayStart + (360 * 1000L); // 00:06:00
        ReplicationRound result6 = manager.getReplicationRoundFromEndTime(multipleRoundsLater);
        assertEquals("Multiple rounds later should be validated and unchanged", multipleRoundsLater, result6.getEndTime());
        assertEquals("Round start time should be end time - round duration",
                   multipleRoundsLater - roundDurationMs, result6.getStartTime());

        // Test 7: Just before round boundary
        long justBeforeRound = dayStart + (299 * 1000L); // 00:04:59
        ReplicationRound result7 = manager.getReplicationRoundFromEndTime(justBeforeRound);
        long expectedEnd7 = dayStart + (240 * 1000L); // Should round down to 00:04:00
        long expectedStart7 = expectedEnd7 - roundDurationMs; // Should be 00:03:00
        assertEquals("Just before round boundary should round down", expectedEnd7, result7.getEndTime());
        assertEquals("Round start time should be end time - round duration", expectedStart7, result7.getStartTime());

        // Test 8: Just after round boundary
        long justAfterRound = dayStart + (301 * 1000L); // 00:05:01
        ReplicationRound result8 = manager.getReplicationRoundFromEndTime(justAfterRound);
        long expectedEnd8 = dayStart + (300 * 1000L); // Should round down to 00:05:00
        long expectedStart8 = expectedEnd8 - roundDurationMs; // Should be 00:04:00
        assertEquals("Just after round boundary should round down", expectedEnd8, result8.getEndTime());
        assertEquals("Round start time should be end time - round duration", expectedStart8, result8.getStartTime());

        // Test 9: Current time (should round down to nearest round)
        long currentTime = System.currentTimeMillis();
        ReplicationRound result9 = manager.getReplicationRoundFromEndTime(currentTime);
        Assert.assertTrue("Current time should round down to nearest round", result9.getEndTime() <= currentTime);
        assertEquals("Round end should be a multiple of round duration", 0, (result9.getEndTime() % roundDurationMs));
        assertEquals("Round start time should be end time - round duration",
                   result9.getEndTime() - roundDurationMs, result9.getStartTime());
    }

    @Test
    public void testGetReplicationRoundFromEndTimeWithCustomConfiguration() {
        // Test with custom round duration
        Configuration customConf = HBaseConfiguration.create();
        customConf.setInt(ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 30);

        Path rootPath = new Path(testFolder.getRoot().getAbsolutePath());
        ReplicationShardDirectoryManager customManager = new ReplicationShardDirectoryManager(customConf, rootPath);

        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC
        long roundDurationMs = 30 * 1000L; // 30 seconds in milliseconds

        // Test with 30-second rounds
        long midRound = dayStart + (45 * 1000L); // 00:00:45
        ReplicationRound result = customManager.getReplicationRoundFromEndTime(midRound);
        long expectedEnd = dayStart + (30 * 1000L); // 00:00:30
        long expectedStart = expectedEnd - roundDurationMs; // 00:00:00
        assertEquals("With 30-second rounds, should round down to 30-second boundary", expectedEnd, result.getEndTime());
        assertEquals("Round start time should be end time - round duration", expectedStart, result.getStartTime());

        // Test exact 30-second boundary
        long exactBoundary = dayStart + (60 * 1000L); // 00:01:00
        ReplicationRound result2 = customManager.getReplicationRoundFromEndTime(exactBoundary);
        assertEquals("Exact 30-second boundary should be validated and unchanged", exactBoundary, result2.getEndTime());
        assertEquals("Round start time should be end time - round duration",
                   exactBoundary - roundDurationMs, result2.getStartTime());
    }

    @Test
    public void testDefaultAndCustomConfigurationValues() {
        // Test with default configuration (no custom values set)
        ReplicationShardDirectoryManager defaultManager = new ReplicationShardDirectoryManager(conf, new Path(testFolder.getRoot().getAbsolutePath()));

        // Validate default values
        assertEquals("numShards should use default value when not configured",
                ReplicationShardDirectoryManager.DEFAULT_REPLICATION_NUM_SHARDS, defaultManager.getNumShards());
        assertEquals("replicationRoundDurationSeconds should use default value when not configured",
                ReplicationShardDirectoryManager.DEFAULT_REPLICATION_ROUND_DURATION_SECONDS, defaultManager.getReplicationRoundDurationSeconds());

        // Test with custom configuration values
        Configuration customConf = new Configuration(conf);
        int customNumShards = 64;
        int customReplicationRoundDurationSeconds = 120;

        customConf.setInt(ReplicationShardDirectoryManager.REPLICATION_NUM_SHARDS_KEY, customNumShards);
        customConf.setInt(ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, customReplicationRoundDurationSeconds);

        ReplicationShardDirectoryManager customManager = new ReplicationShardDirectoryManager(customConf, new Path(testFolder.getRoot().getAbsolutePath()));

        // Validate custom values
        assertEquals("numShards should use custom value when configured",
                customNumShards, customManager.getNumShards());
        assertEquals("replicationRoundDurationSeconds should use custom value when configured",
                customReplicationRoundDurationSeconds, customManager.getReplicationRoundDurationSeconds());
    }

    @Test
    public void testGetReplicationRoundFromEndTimeConsistency() {
        // Test that the same input always produces the same outpu
        long timestamp = 1704110400000L; // 2024-01-01 12:00:00 UTC

        ReplicationRound result1 = manager.getReplicationRoundFromEndTime(timestamp);
        ReplicationRound result2 = manager.getReplicationRoundFromEndTime(timestamp);
        ReplicationRound result3 = manager.getReplicationRoundFromEndTime(timestamp);

        assertEquals("Same input should produce same end time", result1.getEndTime(), result2.getEndTime());
        assertEquals("Same input should produce same start time", result1.getStartTime(), result2.getStartTime());
        assertEquals("Same input should produce same end time", result1.getEndTime(), result3.getEndTime());
        assertEquals("Same input should produce same start time", result1.getStartTime(), result3.getStartTime());

        // Test that round duration is consisten
        long roundDuration1 = result1.getEndTime() - result1.getStartTime();
        assertEquals("Round duration should be 60 seconds", 60 * 1000L, roundDuration1);
    }

    @Test
    public void testGetAllShardPaths() {
        testGetAllShardPathsHelper(conf, ReplicationShardDirectoryManager.DEFAULT_REPLICATION_NUM_SHARDS);
    }

    @Test
    public void testGetAllShardPathsWithCustomConfiguration() {
        // Test with custom number of shards
        int customShardCount = 64;
        Configuration customConf = HBaseConfiguration.create();
        customConf.setInt(ReplicationShardDirectoryManager.REPLICATION_NUM_SHARDS_KEY, customShardCount);
        testGetAllShardPathsHelper(customConf, customShardCount);
    }

    private void testGetAllShardPathsHelper(final Configuration conf, final int expectedShardCount) {

        ReplicationShardDirectoryManager replicationShardDirectoryManager = new ReplicationShardDirectoryManager(conf, new Path(testFolder.getRoot().getAbsolutePath()));
        List<Path> shardPaths = replicationShardDirectoryManager.getAllShardPaths();

        // Verify the number of shards
        assertEquals("Should return exactly " + expectedShardCount + " shard paths", expectedShardCount, shardPaths.size());

        // Verify the base path
        String expectedBasePath = testFolder.getRoot().getAbsolutePath() + "/shard";

        // Verify each shard path
        for (int i = 0; i < expectedShardCount; i++) {
            Path shardPath = shardPaths.get(i);
            String expectedShardName = String.format("%03d", i);
            String expectedPath = expectedBasePath + "/" + expectedShardName;

            assertEquals("Shard " + i + " should have correct path", expectedPath, shardPath.toString());
            assertEquals("Shard " + i + " should have correct name", expectedShardName, shardPath.getName());
        }
    }

    @Test
    public void testGetPreviousRound() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Default configuration: 60-second rounds
        long roundDurationMs = 60 * 1000L; // 60 seconds in milliseconds

        // Test 1: First round (00:00:00 to 00:01:00)
        long firstRoundStart = dayStart; // 00:00:00
        long firstRoundEnd = firstRoundStart + roundDurationMs; // 00:01:00
        ReplicationRound firstRound = new ReplicationRound(firstRoundStart, firstRoundEnd);
        ReplicationRound previousRound = manager.getPreviousRound(firstRound);

        // Previous round should end at firstRoundStart (00:00:00), which rounds down to 00:00:00
        // Start time should be end time - round duration = 00:00:00 - 60s = -60s (edge case)
        long expectedEnd = dayStart; // 00:00:00
        long expectedStart = expectedEnd - roundDurationMs; // -60 seconds
        assertEquals("Previous round end time should be start time of current round rounded down",
                   expectedEnd, previousRound.getEndTime());
        assertEquals("Previous round start time should be end time - round duration",
                   expectedStart, previousRound.getStartTime());

        // Test 2: Second round (00:01:00 to 00:02:00)
        long secondRoundStart = dayStart + roundDurationMs; // 00:01:00
        long secondRoundEnd = secondRoundStart + roundDurationMs; // 00:02:00
        ReplicationRound secondRound = new ReplicationRound(secondRoundStart, secondRoundEnd);
        ReplicationRound previousRound2 = manager.getPreviousRound(secondRound);

        // Previous round should end at secondRoundStart (00:01:00), which rounds down to 00:01:00
        // Start time should be 00:01:00 - 60s = 00:00:00
        long expectedEnd2 = secondRoundStart; // 00:01:00
        long expectedStart2 = expectedEnd2 - roundDurationMs; // 00:00:00
        assertEquals("Previous round end time should be start time of current round rounded down",
                   expectedEnd2, previousRound2.getEndTime());
        assertEquals("Previous round start time should be end time - round duration",
                   expectedStart2, previousRound2.getStartTime());

        // Test 3: Round with mid-timestamp start (00:01:30 would round to 00:01:00)
        long midRoundStart = dayStart + (90 * 1000L); // 00:01:30 (not aligned to round boundary)
        long midRoundEnd = midRoundStart + roundDurationMs; // 00:02:30
        ReplicationRound midRound = new ReplicationRound(midRoundStart, midRoundEnd);
        ReplicationRound previousRound3 = manager.getPreviousRound(midRound);

        // Previous round should end at midRoundStart rounded down = 00:01:00
        // Start time should be 00:01:00 - 60s = 00:00:00
        long expectedEnd3 = dayStart + roundDurationMs; // 00:01:00 (rounded down from 00:01:30)
        long expectedStart3 = expectedEnd3 - roundDurationMs; // 00:00:00
        assertEquals("Previous round end time should round down start time of current round",
                   expectedEnd3, previousRound3.getEndTime());
        assertEquals("Previous round start time should be end time - round duration",
                   expectedStart3, previousRound3.getStartTime());

        // Test 4: Multiple rounds later (00:05:00 to 00:06:00)
        long multipleRoundsStart = dayStart + (5 * roundDurationMs); // 00:05:00
        long multipleRoundsEnd = multipleRoundsStart + roundDurationMs; // 00:06:00
        ReplicationRound multipleRounds = new ReplicationRound(multipleRoundsStart, multipleRoundsEnd);
        ReplicationRound previousRound4 = manager.getPreviousRound(multipleRounds);

        // Previous round should end at multipleRoundsStart = 00:05:00
        // Start time should be 00:05:00 - 60s = 00:04:00
        long expectedEnd4 = multipleRoundsStart; // 00:05:00
        long expectedStart4 = expectedEnd4 - roundDurationMs; // 00:04:00
        assertEquals("Previous round end time should be start time of current round",
                   expectedEnd4, previousRound4.getEndTime());
        assertEquals("Previous round start time should be end time - round duration",
                   expectedStart4, previousRound4.getStartTime());

        // Test 5: Verify round duration is consisten
        long previousRoundDuration = previousRound4.getEndTime() - previousRound4.getStartTime();
        assertEquals("Previous round duration should be 60 seconds", roundDurationMs, previousRoundDuration);
    }

    @Test
    public void testGetNextRound() {
        // Use a specific day for consistent testing
        // 2024-01-01 00:00:00 UTC = 1704067200000L
        long dayStart = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Default configuration: 60-second rounds
        long roundDurationMs = 60 * 1000L; // 60 seconds in milliseconds

        // Test 1: First round (00:00:00 to 00:01:00)
        long firstRoundStart = dayStart; // 00:00:00
        long firstRoundEnd = firstRoundStart + roundDurationMs; // 00:01:00
        ReplicationRound firstRound = new ReplicationRound(firstRoundStart, firstRoundEnd);
        ReplicationRound nextRound = manager.getNextRound(firstRound);

        // Next round should start at firstRoundEnd (00:01:00), which rounds down to 00:01:00
        // End time should be 00:01:00 + 60s = 00:02:00
        long expectedStart = firstRoundEnd; // 00:01:00
        long expectedEnd = expectedStart + roundDurationMs; // 00:02:00
        assertEquals("Next round start time should be end time of current round rounded down",
                   expectedStart, nextRound.getStartTime());
        assertEquals("Next round end time should be start time + round duration",
                   expectedEnd, nextRound.getEndTime());

        // Test 2: Second round (00:01:00 to 00:02:00)
        long secondRoundStart = dayStart + roundDurationMs; // 00:01:00
        long secondRoundEnd = secondRoundStart + roundDurationMs; // 00:02:00
        ReplicationRound secondRound = new ReplicationRound(secondRoundStart, secondRoundEnd);
        ReplicationRound nextRound2 = manager.getNextRound(secondRound);

        // Next round should start at secondRoundEnd (00:02:00), which rounds down to 00:02:00
        // End time should be 00:02:00 + 60s = 00:03:00
        long expectedStart2 = secondRoundEnd; // 00:02:00
        long expectedEnd2 = expectedStart2 + roundDurationMs; // 00:03:00
        assertEquals("Next round start time should be end time of current round rounded down",
                   expectedStart2, nextRound2.getStartTime());
        assertEquals("Next round end time should be start time + round duration",
                   expectedEnd2, nextRound2.getEndTime());

        // Test 3: Round with mid-timestamp end (00:02:30 would round to 00:02:00)
        long midRoundStart = dayStart + (120 * 1000L); // 00:02:00
        long midRoundEnd = dayStart + (150 * 1000L); // 00:02:30 (not aligned to round boundary)
        ReplicationRound midRound = new ReplicationRound(midRoundStart, midRoundEnd);
        ReplicationRound nextRound3 = manager.getNextRound(midRound);

        // Next round should start at midRoundEnd rounded down = 00:02:00
        // End time should be 00:02:00 + 60s = 00:03:00
        long expectedStart3 = dayStart + (120 * 1000L); // 00:02:00 (rounded down from 00:02:30)
        long expectedEnd3 = expectedStart3 + roundDurationMs; // 00:03:00
        assertEquals("Next round start time should round down end time of current round",
                   expectedStart3, nextRound3.getStartTime());
        assertEquals("Next round end time should be start time + round duration",
                   expectedEnd3, nextRound3.getEndTime());

        // Test 4: Multiple rounds later (00:05:00 to 00:06:00)
        long multipleRoundsStart = dayStart + (5 * roundDurationMs); // 00:05:00
        long multipleRoundsEnd = multipleRoundsStart + roundDurationMs; // 00:06:00
        ReplicationRound multipleRounds = new ReplicationRound(multipleRoundsStart, multipleRoundsEnd);
        ReplicationRound nextRound4 = manager.getNextRound(multipleRounds);

        // Next round should start at multipleRoundsEnd = 00:06:00
        // End time should be 00:06:00 + 60s = 00:07:00
        long expectedStart4 = multipleRoundsEnd; // 00:06:00
        long expectedEnd4 = expectedStart4 + roundDurationMs; // 00:07:00
        assertEquals("Next round start time should be end time of current round",
                   expectedStart4, nextRound4.getStartTime());
        assertEquals("Next round end time should be start time + round duration",
                   expectedEnd4, nextRound4.getEndTime());

        // Test 5: Verify round duration is consisten
        long nextRoundDuration = nextRound4.getEndTime() - nextRound4.getStartTime();
        assertEquals("Next round duration should be 60 seconds", roundDurationMs, nextRoundDuration);

        // Test 6: Verify continuity - next round of previous round should equal original round
        ReplicationRound originalRound = new ReplicationRound(dayStart + (3 * roundDurationMs), dayStart + (4 * roundDurationMs)); // 00:03:00 to 00:04:00
        ReplicationRound prevRound = manager.getPreviousRound(originalRound);
        ReplicationRound nextOfPrev = manager.getNextRound(prevRound);
        assertEquals("Next round of previous round should equal original round start time",
                   originalRound.getStartTime(), nextOfPrev.getStartTime());
        assertEquals("Next round of previous round should equal original round end time",
                   originalRound.getEndTime(), nextOfPrev.getEndTime());
    }
}
