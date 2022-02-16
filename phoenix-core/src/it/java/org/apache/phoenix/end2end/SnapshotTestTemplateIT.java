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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a not a standard IT.
 * It is starting point for writing ITs that load specific tables from a snapshot.
 * Tests based on this IT are meant for debugging specific problems where HBase table snapshots are
 * available for replication, and are not meant to be part of the standard test suite
 * (or even being committed to the ASF branches)
 */

@Ignore
@Category(NeedsOwnMiniClusterTest.class)
public class SnapshotTestTemplateIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        SnapshotTestTemplateIT.class);

    private static final HashMap<String, String> SNAPSHOTS_TO_LOAD;

    static {
        SNAPSHOTS_TO_LOAD = new HashMap<>();
        //Add any HBase tables, including Phoenix System tables
        SNAPSHOTS_TO_LOAD.put("SYSTEM.CATALOG_SNAPSHOT", "/path/to/local/snapshot_dir");
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");

        //Start minicluster without Phoenix first
        checkClusterInitialized(new ReadOnlyProps(serverProps.entrySet().iterator()));

        //load snapshots int HBase
        for (Entry<String, String> snapshot : SNAPSHOTS_TO_LOAD.entrySet()) {
            importSnapshot(snapshot.getKey(), snapshot.getValue());
        }

        //Now we can start Phoenix
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet()
                .iterator()));
    }

    private static void importSnapshot(String key, String value) throws IOException {
        LOGGER.info("importing {} snapshot from {}", key, value);
        // copy local snapshot dir to Minicluster HDFS
        Path localPath = new Path(value);
        assertTrue(FileSystem.getLocal(config).exists(new Path(localPath, ".hbase-snapshot")));
        FileSystem hdfsFs = FileSystem.get(config);
        Path hdfsImportPath = new Path(hdfsFs.getHomeDirectory(), "snapshot-import" + "/" + key + "/");
        assertTrue(hdfsFs.mkdirs(hdfsImportPath));
        hdfsFs.copyFromLocalFile(localPath, hdfsImportPath);
        hdfsImportPath = new Path(hdfsImportPath, localPath.getName());
        assertTrue(hdfsFs.exists(new Path(hdfsImportPath, ".hbase-snapshot")));

        //import the snapshot
        ExportSnapshot exportTool = new ExportSnapshot();
        exportTool.setConf(config);
        int importExitCode = exportTool.run(new String[] {
                "-snapshot", key,
                "-copy-from", hdfsImportPath.toUri().toString(),
                "-copy-to", CommonFSUtils.getRootDir(config).toUri().toString()
                });
        assertEquals(0, importExitCode);

        //load the snapshot
        utility.getHBaseAdmin().restoreSnapshot(key);
    }

    @Test
    public void testDummy() throws Exception {
        assertTrue(true);
    }

}
