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
package org.apache.phoenix.replication.reader;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationLogReplayFileTrackerTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

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
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testGetNewLogSubDirectoryName() {
        // Create ReplicationLogReplayFileTracker instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, haGroupName, localFs, rootURI);

        // Test that it returns the same string as IN_SUBDIRECTORY constant
        String result = fileTracker.getNewLogSubDirectoryName();
        assertEquals("Should return the same string as IN_SUBDIRECTORY constant",
            ReplicationLogReplayFileTracker.IN_SUBDIRECTORY, result);
    }

}
