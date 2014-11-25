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
package org.apache.phoenix.hbase.index;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.covered.example.ColumnGroup;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumn;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumnIndexSpecifierBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we correctly fail for versions of HBase that don't support current properties
 */
@Category(NeedsOwnMiniClusterTest.class)
public class FailForUnsupportedHBaseVersionsIT {
    private static final Log LOG = LogFactory.getLog(FailForUnsupportedHBaseVersionsIT.class);

    /**
     * We don't support WAL Compression for HBase &lt; 0.94.9, so we shouldn't even allow the server
     * to start if both indexing and WAL Compression are enabled for the wrong versions.
     */
    @Test
    public void testDoesNotSupportCompressedWAL() {
        Configuration conf = HBaseConfiguration.create();
        IndexTestingUtils.setupConfig(conf);
        // get the current version
        String version = VersionInfo.getVersion();

        // ensure WAL Compression not enabled
        conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);

        //we support all versions without WAL Compression
        String supported = Indexer.validateVersion(version, conf);
        assertNull(
                "WAL Compression wasn't enabled, but version "+version+" of HBase wasn't supported! All versions should"
                        + " support writing without a compressed WAL. Message: "+supported, supported);

        // enable WAL Compression
        conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);

        // set the version to something we know isn't supported
        version = "0.94.4";
        supported = Indexer.validateVersion(version, conf);
        assertNotNull("WAL Compression was enabled, but incorrectly marked version as supported",
                supported);

        //make sure the first version of 0.94 that supports Indexing + WAL Compression works
        version = "0.94.9";
        supported = Indexer.validateVersion(version, conf);
        assertNull(
                "WAL Compression wasn't enabled, but version "+version+" of HBase wasn't supported! Message: "+supported, supported);

        //make sure we support snapshot builds too
        version = "0.94.9-SNAPSHOT";
        supported = Indexer.validateVersion(version, conf);
        assertNull(
                "WAL Compression wasn't enabled, but version "+version+" of HBase wasn't supported! Message: "+supported, supported);
    }

    /**
     * Test that we correctly abort a RegionServer when we run tests with an unsupported HBase
     * version. The 'completeness' of this test requires that we run the test with both a version of
     * HBase that wouldn't be supported with WAL Compression. Currently, this is the default version
     * (0.94.4) so just running 'mvn test' will run the full test. However, this test will not fail
     * when running against a version of HBase with WALCompression enabled. Therefore, to fully test
     * this functionality, we need to run the test against both a supported and an unsupported version
     * of HBase (as long as we want to support an version of HBase that doesn't support custom WAL
     * Codecs).
     * @throws Exception on failure
     */
    @Test(timeout = 300000 /* 5 mins */)
    public void testDoesNotStartRegionServerForUnsupportedCompressionAndVersion() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        IndexTestingUtils.setupConfig(conf);
        // enable WAL Compression
        conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);

        // check the version to see if it isn't supported
        String version = VersionInfo.getVersion();
        boolean supported = false;
        if (Indexer.validateVersion(version, conf) == null) {
            supported = true;
        }

        // start the minicluster
        HBaseTestingUtility util = new HBaseTestingUtility(conf);
        util.startMiniCluster();

        try {
            // setup the primary table
            @SuppressWarnings("deprecation")
            HTableDescriptor desc = new HTableDescriptor(
                    "testDoesNotStartRegionServerForUnsupportedCompressionAndVersion");
            byte[] family = Bytes.toBytes("f");
            desc.addFamily(new HColumnDescriptor(family));

            // enable indexing to a non-existant index table
            String indexTableName = "INDEX_TABLE";
            ColumnGroup fam1 = new ColumnGroup(indexTableName);
            fam1.add(new CoveredColumn(family, CoveredColumn.ALL_QUALIFIERS));
            CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
            builder.addIndexGroup(fam1);
            builder.build(desc);

            // get a reference to the regionserver, so we can ensure it aborts
            HRegionServer server = util.getMiniHBaseCluster().getRegionServer(0);

            // create the primary table
            HBaseAdmin admin = util.getHBaseAdmin();
            if (supported) {
                admin.createTable(desc);
                assertFalse("Hosting regeion server failed, even the HBase version (" + version
                        + ") supports WAL Compression.", server.isAborted());
            } else {
                admin.createTableAsync(desc, null);

                // wait for the regionserver to abort - if this doesn't occur in the timeout, assume its
                // broken.
                while (!server.isAborted()) {
                    LOG.debug("Waiting on regionserver to abort..");
                }
            }

        } finally {
            // cleanup
            util.shutdownMiniCluster();
        }
    }
}