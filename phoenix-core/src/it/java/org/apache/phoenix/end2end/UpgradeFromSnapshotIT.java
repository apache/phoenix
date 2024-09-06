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

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This is a not a standard IT.
 * It is starting point for writing ITs that load specific tables from a snapshot.
 * Tests based on this IT are meant for debugging specific problems where HBase table snapshots are
 * available for replication, and are not meant to be part of the standard test suite
 * (or even being committed to the ASF branches)
 */

@Category(NeedsOwnMiniClusterTest.class)
public class UpgradeFromSnapshotIT extends LoadSystemTableSnapshotBase {
    static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION", "SYSTEM:MUTEX", "SYSTEM:CHILD_LINK","SYSTEM:TRANSFORM"));

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        boolean blockUpgrade = false;
        setupCluster(blockUpgrade);
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

        // Check the System Tables after upgrade
        Set<String> tables = new HashSet<>();
        for (TableName tn : utility.getAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
                tables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));

    }
}
