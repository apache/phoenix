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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.phoenix.replication.reader.RecoverLeaseFSUtils.LEASE_RECOVERABLE_CLASS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test our recoverLease loop against mocked up filesystem.
 */
public class RecoverLeaseFSUtilsTest extends ParallelStatsDisabledIT {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private static Configuration conf;
    private static FileSystem localFs;
    private static Path FILE;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        conf = getUtility().getConfiguration();
        localFs = FileSystem.getLocal(conf);
        conf.setLong(RecoverLeaseFSUtils.REPLICATION_REPLAY_LEASE_RECOVERY_FIRST_PAUSE_MILLISECOND, 10);
        conf.setLong(RecoverLeaseFSUtils.REPLICATION_REPLAY_LEASE_RECOVERY_PAUSE_MILLISECOND, 10);
        FILE = new Path(testFolder.newFile("file.txt").toURI());
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    /**
     * Test recover lease eventually succeeding.
     */
    @Test
    public void testRecoverLease() throws IOException {
        long startTime = EnvironmentEdgeManager.currentTime();
        conf.setLong(RecoverLeaseFSUtils.REPLICATION_REPLAY_LEASE_RECOVERY_DFS_TIMEOUT_MILLISECOND, 1000);
        CancelableProgressable reporter = Mockito.mock(CancelableProgressable.class);
        Mockito.when(reporter.progress()).thenReturn(true);
        DistributedFileSystem dfs = Mockito.mock(DistributedFileSystem.class);
        // Fail four times and pass on the fifth.
        Mockito.when(dfs.recoverLease(FILE)).thenReturn(false).thenReturn(false).thenReturn(false)
                .thenReturn(false).thenReturn(true);
        RecoverLeaseFSUtils.recoverFileLease(dfs, FILE, conf, reporter);
        Mockito.verify(dfs, Mockito.times(5)).recoverLease(FILE);
        // Make sure we waited at least hbase.lease.recovery.dfs.timeout * 3 (the first two
        // invocations will happen pretty fast... then we fall into the longer wait loop).
        assertTrue((EnvironmentEdgeManager.currentTime() - startTime)
                > (3 * conf.getLong(RecoverLeaseFSUtils.REPLICATION_REPLAY_LEASE_RECOVERY_DFS_TIMEOUT_MILLISECOND, 61000)));
    }

    /**
     * Test that we can use reflection to access LeaseRecoverable methods.
     */
    @Test
    public void testLeaseRecoverable() throws IOException {
        try {
            // set LeaseRecoverable to FakeLeaseRecoverable for testing
            RecoverLeaseFSUtils.initializeRecoverLeaseMethod(FakeLeaseRecoverable.class.getName());
            RecoverableFileSystem mockFS = Mockito.mock(RecoverableFileSystem.class);
            Mockito.when(mockFS.recoverLease(FILE)).thenReturn(true);
            RecoverLeaseFSUtils.recoverFileLease(mockFS, FILE, conf);
            Mockito.verify(mockFS, Mockito.times(1)).recoverLease(FILE);
            assertTrue(RecoverLeaseFSUtils.isLeaseRecoverable(Mockito.mock(RecoverableFileSystem.class)));
        } finally {
            RecoverLeaseFSUtils.initializeRecoverLeaseMethod(LEASE_RECOVERABLE_CLASS_NAME);
        }
    }

    /**
     * Test that isFileClosed makes us recover lease faster.
     */
    @Test
    public void testIsFileClosed() throws IOException {
        // Make this time long so it is plain we broke out because of the isFileClosed invocation.
        conf.setLong(RecoverLeaseFSUtils.REPLICATION_REPLAY_LEASE_RECOVERY_DFS_TIMEOUT_MILLISECOND, 100000);
        CancelableProgressable reporter = Mockito.mock(CancelableProgressable.class);
        Mockito.when(reporter.progress()).thenReturn(true);
        IsFileClosedDistributedFileSystem dfs = Mockito.mock(IsFileClosedDistributedFileSystem.class);
        // Now make it so we fail the first two times -- the two fast invocations, then we fall into
        // the long loop during which we will call isFileClosed.... the next invocation should
        // therefore return true if we are to break the loop.
        Mockito.when(dfs.recoverLease(FILE)).thenReturn(false).thenReturn(false).thenReturn(true);
        Mockito.when(dfs.isFileClosed(FILE)).thenReturn(true);
        RecoverLeaseFSUtils.recoverFileLease(dfs, FILE, conf, reporter);
        Mockito.verify(dfs, Mockito.times(2)).recoverLease(FILE);
        Mockito.verify(dfs, Mockito.times(1)).isFileClosed(FILE);
    }

    /**
     * Test isLeaseRecoverable for both distributed and local FS
     */
    @Test
    public void testIsLeaseRecoverable() {
        assertTrue(RecoverLeaseFSUtils.isLeaseRecoverable(new DistributedFileSystem()));
        assertFalse(RecoverLeaseFSUtils.isLeaseRecoverable(new LocalFileSystem()));
    }

    private interface FakeLeaseRecoverable {
        @SuppressWarnings("unused")
        boolean recoverLease(Path p) throws IOException;

        @SuppressWarnings("unused")
        boolean isFileClosed(Path p) throws IOException;
    }

    private static abstract class RecoverableFileSystem extends FileSystem
            implements FakeLeaseRecoverable {
        @Override
        public boolean recoverLease(Path p) throws IOException {
            return true;
        }

        @Override
        public boolean isFileClosed(Path p) throws IOException {
            return true;
        }
    }

    /**
     * Version of DFS that has HDFS-4525 in it.
     */
    private static class IsFileClosedDistributedFileSystem extends DistributedFileSystem {
        /**
         * Close status of a file. Copied over from HDFS-4525
         * @return true if file is already closed
         **/
        @Override
        public boolean isFileClosed(Path f) throws IOException {
            return false;
        }
    }
}
