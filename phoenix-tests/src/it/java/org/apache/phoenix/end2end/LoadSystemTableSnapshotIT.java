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


import java.io.File;
import java.io.IOException;

import java.net.URL;

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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;


import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * This is a not a standard IT.
 * It is starting point for writing ITs that load specific tables from a snapshot.
 * Tests based on this IT are meant for debugging specific problems where HBase table snapshots are
 * available for replication, and are not meant to be part of the standard test suite
 * (or even being committed to the ASF branches)
 */

@Category(NeedsOwnMiniClusterTest.class)
public class LoadSystemTableSnapshotIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(
            LoadSystemTableSnapshotIT.class);

    public static final String SNAPSHOT_DIR = "snapshots4_7/";
    public static String rootDir;

    private static final HashMap<String, String> SNAPSHOTS_TO_LOAD;
//    private static final HashMap<String, String> SNAPSHOTS_TO_RESTORE;

    static {
        SNAPSHOTS_TO_LOAD = new HashMap<>();
        //Add any HBase tables, including Phoenix System tables

        SNAPSHOTS_TO_LOAD.put("SYSTEM.CATALOG_SNAPSHOT", "SYSTEM.CATALOG");
        SNAPSHOTS_TO_LOAD.put("SYSTEM.FUNCTION_SNAPSHOT", "SYSTEM.FUNCTION");
        SNAPSHOTS_TO_LOAD.put("SYSTEM.SEQUENCE_SNAPSHOT", "SYSTEM.SEQUENCE");
        SNAPSHOTS_TO_LOAD.put("SYSTEM.STATS_SNAPSHOT", "SYSTEM.STATS");
    }

    private static void decompress(String in, File out) throws IOException {
        try (TarArchiveInputStream fin = new TarArchiveInputStream(new FileInputStream(in))){
            TarArchiveEntry entry;
            while ((entry = fin.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                File curfile = new File(out, entry.getName());
                File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                IOUtils.copy(fin, new FileOutputStream(curfile));
            }
        }
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

        URL folderUrl = LoadSystemTableSnapshotIT.class.getClassLoader()
                .getResource(SNAPSHOT_DIR);

        // extract the tar

        File archive = new File(folderUrl.getFile() + "snapshots47.tar.gz");
        File destination = new File(folderUrl.getFile());

        decompress(archive.toString(), destination);

        //load snapshots int HBase
        rootDir = CommonFSUtils.getRootDir(config).toUri().toString();

        for (Entry<String, String> snapshot : SNAPSHOTS_TO_LOAD.entrySet()) {
            String snapshotLoc = new File(folderUrl.getFile()).getAbsolutePath() + "/" + snapshot.getKey();
            importSnapshot(snapshot.getKey(), snapshot.getValue(), snapshotLoc);
        }
    }

    private static void importSnapshot(String key, String value, String loc) throws IOException {
        LOGGER.info("importing {} snapshot from {}", key, value);
        // copy local snapshot dir to Minicluster HDFS
        Path localPath = new Path(loc);
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
                "-copy-to", rootDir
                });
        assertEquals(0, importExitCode);

        //load the snapshot
        utility.getAdmin().restoreSnapshot(key);
    }

    @Test
    public void testPhoenixUpgrade() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");

        //Now we can start Phoenix
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet()
                .iterator()));
        assertTrue(true);
    }

}
