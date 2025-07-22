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
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

public class ReplicationLogDiscoveryServiceTest extends ParallelStatsDisabledIT {

    private static final String CREATE_TABLE_SQL_STATEMENT = "CREATE TABLE %s (ID VARCHAR PRIMARY KEY, " +
            "COL_1 VARCHAR, COL_2 VARCHAR, COL_3 BIGINT)";

    private static final String UPSERT_SQL_STATEMENT = "upsert into %s values ('%s', '%s', '%s', %s)";

    private static final String PRINCIPAL = "replicationLogServiceTest";

    private static final String testHAGroupId = "testHAGroupId";

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private static Configuration conf;
    private static FileSystem localFs;

    private static ReplicationLogReplayService replicationLogReplayService;



    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        conf = getUtility().getConfiguration();
        localFs = FileSystem.getLocal(conf);
        conf.set(ReplicationReplay.REPLICATION_LOG_REPLAY_HDFS_URL_KEY, testFolder.toString());
        replicationLogReplayService = Mockito.spy(ReplicationLogReplayService.getInstance(conf));
        Mockito.doReturn(Collections.singletonList(testHAGroupId)).when(replicationLogReplayService).getReplicationGroups();
    }

    @Test
    public void test() throws IOException, InterruptedException {
//        ReplicationLogReplayService replicationLogReplayService = ReplicationLogReplayService.getInstance(conf);
//        replicationLogReplayService.init();
//        replicationLogReplayService.start();
//        ReplicationLogReplay replicationLogReplay = new ReplicationLogReplay(conf, testHAGroupId);
//        replicationLogReplay.init();
//        replicationLogReplay.replay();
//        Thread.sleep(5 * 1000L * 60);
    }

}
