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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.query.QueryServices.HA_STORE_AND_FORWARD_MODE_REFRESH_INTERVAL_MS;
import static org.apache.phoenix.query.QueryServices.HA_SYNC_MODE_REFRESH_INTERVAL_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.util.JacksonUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Additional imports for configuration and exceptions
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;

/**
 * Unit test for {@link ClusterRoleRecord}.
 */
@RunWith(Parameterized.class)
public class ClusterRoleRecordTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecordTest.class);
    private static final String URL1 = "zk1-1\\:2181,zk1-2\\:2181";
    private static final String URL2 = "zk2-1\\:2181,zk2-2\\:2181";
    private final ClusterRoleRecord.RegistryType registryType;

    @Rule
    public final TestName testName = new TestName();

    public ClusterRoleRecordTest(ClusterRoleRecord.RegistryType registryType) {
        this.registryType = registryType;
    }

    @Parameterized.Parameters(name="ClusterRoleRecord_registryType={0}")
    public static Collection<Object> data() {
        return Arrays.asList(new Object[] {
                ClusterRoleRecord.RegistryType.ZK,
                ClusterRoleRecord.RegistryType.MASTER,
                ClusterRoleRecord.RegistryType.RPC,
                null //For Backward Compatibility
        });
    }

    /**
     * Helper method to create a temp JSON file with the given array of cluster role records.
     */
    public static String createJsonFileWithRecords(ClusterRoleRecord... records)
            throws IOException {
        File file = File.createTempFile("phoenix.ha.cluster.role.records", ".test.json");
        file.deleteOnExit();
        JacksonUtil.getObjectWriterPretty().writeValue(file, records);
        LOG.info("Prepared the JSON file for testing, file:{}, content:\n{}", file,
                FileUtils.readFileToString(file, "UTF-8"));
        return file.getPath();
    }

    @Before
    public void setUp() throws Exception {
        if (registryType == ClusterRoleRecord.RegistryType.RPC) {
            assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0")>=0);
        }
    }

    @Test
    public void testReadWriteJsonToFile() throws IOException {
        ClusterRoleRecord record = getClusterRoleRecord(testName.getMethodName(),
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY, 1);
        String fileName = createJsonFileWithRecords(record);
        String fileContent = FileUtils.readFileToString(new File(fileName), "UTF-8");
        assertTrue(fileContent.contains(record.getHaGroupName()));
    }

    @Test
    public void testOldFormatCompatibility() throws IOException {
        String oldFormatPath = "json/test_role_record_old_format.json";
        String newFormatPath = "json/test_role_record.json";
        byte[] oldFormat = readFile(oldFormatPath);
        byte[] newFormat = readFile(newFormatPath);
        assertFalse(Arrays.equals(oldFormat, newFormat));

        ClusterRoleRecord oldFormatCRR = ClusterRoleRecord.fromJson(oldFormat).get();
        ClusterRoleRecord newFormatCRR = ClusterRoleRecord.fromJson(newFormat).get();
        assertEquals(oldFormatCRR, newFormatCRR);

    }

    @Test
    public void testUrlNullInRoleRecord() throws IOException {
        String wrongFormatPath = "json/test_role_record_wrong_format.json";
        String wrongFormatForRolePath = "json/test_role_record_wrong_role_format.json";
        byte[] roleRecord = readFile(wrongFormatPath);
        byte[] roleRecordWithWrongRole = readFile(wrongFormatForRolePath);

        Optional<ClusterRoleRecord> record = ClusterRoleRecord.fromJson(roleRecord);
        //We should get empty object as url is missing in ClusterRoleRecord
        assertEquals(record, Optional.empty());


        Optional<ClusterRoleRecord> recordWithNullRole = ClusterRoleRecord.fromJson(roleRecordWithWrongRole);
        //We should get empty object as role is missing in ClusterRoleRecord
        assertEquals(record, Optional.empty());
    }

    @Test
    public void testToAndFromJson() throws IOException {
        ClusterRoleRecord record = getClusterRoleRecord(testName.getMethodName(),
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,1);
        byte[] bytes = ClusterRoleRecord.toJson(record);
        Optional<ClusterRoleRecord> record2 = ClusterRoleRecord.fromJson(bytes);
        assertTrue(record2.isPresent());
        assertEquals(record, record2.get());
    }

    @Test
    public void testGetActiveUrl() {
        ClusterRoleRecord record = getClusterRoleRecord(testName.getMethodName(),
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,0);
        assertTrue(record.getActiveUrl().isPresent());
        assertEquals(getUrlWithSuffix(URL1), record.getActiveUrl().get());

        record = getClusterRoleRecord(testName.getMethodName(),
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.STANDBY,
                URL2, ClusterRole.STANDBY, 0);
        assertFalse(record.getActiveUrl().isPresent());

    }

    @Test
    public void testIsNewerThan() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.STANDBY,
                URL2, ClusterRole.STANDBY,0);
        ClusterRoleRecord recordV1 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.STANDBY,
                URL2, ClusterRole.STANDBY,2);
        assertTrue(recordV1.isNewerThan(recordV0));  // v1 is indeed newer
        assertFalse(recordV1.isNewerThan(recordV1)); // irreflexive
        assertFalse(recordV0.isNewerThan(recordV1)); // antisymmetry

        // Create a new cluster role record for a new HA group name.
        // Cluster role records for different HA groups can not compare in reality,
        // so they are not newer than each other.
        String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
        ClusterRoleRecord record2 = getClusterRoleRecord(haGroupName2,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.STANDBY,
                URL2, ClusterRole.STANDBY, 1);
        assertFalse(recordV0.isNewerThan(record2));
        assertFalse(recordV1.isNewerThan(record2));
        assertFalse(record2.isNewerThan(recordV0));
        assertFalse(record2.isNewerThan(recordV1));
    }

    @Test
    public void testHasSameInfo() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,0);
        ClusterRoleRecord recordV1 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY, 1);
        assertTrue(recordV1.hasSameInfo(recordV0));
        assertTrue(recordV1.hasSameInfo(recordV1));
        assertTrue(recordV0.hasSameInfo(recordV1));
    }

    @Test
    public void testHasSameInfoDifferentZKOrder() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL2, ClusterRole.ACTIVE,
                URL1, ClusterRole.STANDBY, 0);
        ClusterRoleRecord recordV1 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY, 1);
        assertTrue(recordV1.hasSameInfo(recordV0));
        assertTrue(recordV1.hasSameInfo(recordV1));
        assertTrue(recordV0.hasSameInfo(recordV1));
    }

    @Test
    public void testHasSameInfoDifferentHostOrder() {
        String hostzk1ordered = "zk1-1,zk1-2:2181";
        String hostzk1unordered = "zk1-2,zk1-1:2181";
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL2, ClusterRole.ACTIVE,
                hostzk1ordered, ClusterRole.STANDBY, 0);
        ClusterRoleRecord recordV1 = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, hostzk1unordered, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,1);
        assertTrue(recordV1.hasSameInfo(recordV0));
        assertTrue(recordV1.hasSameInfo(recordV1));
        assertTrue(recordV0.hasSameInfo(recordV1));
    }

    @Test
    public void testHasSameInfoNegative() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord record = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.PARALLEL, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,0);

        ClusterRoleRecord recordFailover = getClusterRoleRecord(haGroupName,
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,1);
        assertFalse(record.hasSameInfo(recordFailover));
        assertFalse(recordFailover.hasSameInfo(record));

        String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
        ClusterRoleRecord record2 = getClusterRoleRecord(haGroupName2,
                HighAvailabilityPolicy.PARALLEL, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,1);
        assertFalse(record.hasSameInfo(record2));
        assertFalse(record2.hasSameInfo(record));
    }

    @Test
    public void testGetRole() {
        ClusterRoleRecord record = getClusterRoleRecord(testName.getMethodName(),
                HighAvailabilityPolicy.FAILOVER, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,0);
        assertEquals(ClusterRole.ACTIVE, record.getRole(getUrlWithSuffix(URL1)));
        assertEquals(ClusterRole.ACTIVE, record.getRole(record.getUrl1()));
        assertEquals(ClusterRole.STANDBY, record.getRole(record.getUrl2()));
        assertEquals(ClusterRole.UNKNOWN, record.getRole(null));
        assertEquals(ClusterRole.UNKNOWN, record.getRole("foo"));
    }

    @Test
    public void testToPrettyString() {
        ClusterRoleRecord record =  getClusterRoleRecord(testName.getMethodName(),
                HighAvailabilityPolicy.PARALLEL, URL1, ClusterRole.ACTIVE,
                URL2, ClusterRole.STANDBY,1);
        LOG.info("toString(): {}", record.toString());
        LOG.info("toPrettyString:\n{}", record.toPrettyString());
        assertNotEquals(record.toString(), record.toPrettyString());
    }

    // Tests for ClusterRole enum

    @Test
    public void testClusterRoleValues() {
        ClusterRole[] expectedRoles = {
                ClusterRole.ABORT_TO_ACTIVE,
                ClusterRole.ABORT_TO_STANDBY,
                ClusterRole.ACTIVE,
                ClusterRole.ACTIVE_NOT_IN_SYNC,
                ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY,
                ClusterRole.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER,
                ClusterRole.ACTIVE_TO_STANDBY,
                ClusterRole.ACTIVE_WITH_OFFLINE_PEER,
                ClusterRole.DEGRADED_STANDBY,
                ClusterRole.DEGRADED_STANDBY_FOR_READER,
                ClusterRole.DEGRADED_STANDBY_FOR_WRITER,
                ClusterRole.OFFLINE,
                ClusterRole.STANDBY,
                ClusterRole.STANDBY_TO_ACTIVE,
                ClusterRole.UNKNOWN
        };

        ClusterRole[] actualRoles = ClusterRole.values();
        assertEquals("Number of ClusterRole values", expectedRoles.length, actualRoles.length);

        for (int i = 0; i < expectedRoles.length; i++) {
            assertEquals("ClusterRole at index " + i, expectedRoles[i], actualRoles[i]);
        }
    }

    @Test
    public void testClusterRoleValueOf() {
        assertEquals(ClusterRole.ABORT_TO_ACTIVE, ClusterRole.valueOf("ABORT_TO_ACTIVE"));
        assertEquals(ClusterRole.ABORT_TO_STANDBY, ClusterRole.valueOf("ABORT_TO_STANDBY"));
        assertEquals(ClusterRole.ACTIVE, ClusterRole.valueOf("ACTIVE"));
        assertEquals(ClusterRole.ACTIVE_NOT_IN_SYNC, ClusterRole.valueOf("ACTIVE_NOT_IN_SYNC"));
        assertEquals(ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY, ClusterRole.valueOf("ACTIVE_NOT_IN_SYNC_TO_STANDBY"));
        assertEquals(ClusterRole.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER, ClusterRole.valueOf("ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER"));
        assertEquals(ClusterRole.ACTIVE_TO_STANDBY, ClusterRole.valueOf("ACTIVE_TO_STANDBY"));
        assertEquals(ClusterRole.ACTIVE_WITH_OFFLINE_PEER, ClusterRole.valueOf("ACTIVE_WITH_OFFLINE_PEER"));
        assertEquals(ClusterRole.DEGRADED_STANDBY, ClusterRole.valueOf("DEGRADED_STANDBY"));
        assertEquals(ClusterRole.DEGRADED_STANDBY_FOR_READER, ClusterRole.valueOf("DEGRADED_STANDBY_FOR_READER"));
        assertEquals(ClusterRole.DEGRADED_STANDBY_FOR_WRITER, ClusterRole.valueOf("DEGRADED_STANDBY_FOR_WRITER"));
        assertEquals(ClusterRole.OFFLINE, ClusterRole.valueOf("OFFLINE"));
        assertEquals(ClusterRole.STANDBY, ClusterRole.valueOf("STANDBY"));
        assertEquals(ClusterRole.STANDBY_TO_ACTIVE, ClusterRole.valueOf("STANDBY_TO_ACTIVE"));
        assertEquals(ClusterRole.UNKNOWN, ClusterRole.valueOf("UNKNOWN"));
    }

    @Test
    public void testClusterRoleValueOfInvalid() {
        try {
            ClusterRole.valueOf("INVALID_ROLE");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            ClusterRole.valueOf(null);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testClusterRoleCanConnect() {
        // Roles that can connect
        assertTrue(ClusterRole.ABORT_TO_ACTIVE.canConnect());
        assertTrue(ClusterRole.ABORT_TO_STANDBY.canConnect());
        assertTrue(ClusterRole.ACTIVE.canConnect());
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC.canConnect());
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY.canConnect());
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER.canConnect());
        assertTrue(ClusterRole.ACTIVE_TO_STANDBY.canConnect());
        assertTrue(ClusterRole.ACTIVE_WITH_OFFLINE_PEER.canConnect());
        assertTrue(ClusterRole.DEGRADED_STANDBY.canConnect());
        assertTrue(ClusterRole.DEGRADED_STANDBY_FOR_READER.canConnect());
        assertTrue(ClusterRole.DEGRADED_STANDBY_FOR_WRITER.canConnect());
        assertTrue(ClusterRole.STANDBY.canConnect());
        assertTrue(ClusterRole.STANDBY_TO_ACTIVE.canConnect());

        // Roles that cannot connect
        assertFalse(ClusterRole.OFFLINE.canConnect());
        assertFalse(ClusterRole.UNKNOWN.canConnect());
    }

    @Test
    public void testClusterRoleIsActive() {
        // Active roles
        assertTrue(ClusterRole.ACTIVE.isActive());
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC.isActive());
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY.isActive());
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER.isActive());
        assertTrue(ClusterRole.ACTIVE_TO_STANDBY.isActive());
        assertTrue(ClusterRole.ACTIVE_WITH_OFFLINE_PEER.isActive());

        // Non-active roles
        assertFalse(ClusterRole.ABORT_TO_ACTIVE.isActive());
        assertFalse(ClusterRole.ABORT_TO_STANDBY.isActive());
        assertFalse(ClusterRole.DEGRADED_STANDBY.isActive());
        assertFalse(ClusterRole.DEGRADED_STANDBY_FOR_READER.isActive());
        assertFalse(ClusterRole.DEGRADED_STANDBY_FOR_WRITER.isActive());
        assertFalse(ClusterRole.OFFLINE.isActive());
        assertFalse(ClusterRole.STANDBY.isActive());
        assertFalse(ClusterRole.STANDBY_TO_ACTIVE.isActive());
        assertFalse(ClusterRole.UNKNOWN.isActive());
    }

    @Test
    public void testClusterRoleIsStandby() {
        // Standby roles
        assertTrue(ClusterRole.DEGRADED_STANDBY.isStandby());
        assertTrue(ClusterRole.DEGRADED_STANDBY_FOR_READER.isStandby());
        assertTrue(ClusterRole.DEGRADED_STANDBY_FOR_WRITER.isStandby());
        assertTrue(ClusterRole.STANDBY.isStandby());
        assertTrue(ClusterRole.STANDBY_TO_ACTIVE.isStandby());

        // Non-standby roles
        assertFalse(ClusterRole.ABORT_TO_ACTIVE.isStandby());
        assertFalse(ClusterRole.ABORT_TO_STANDBY.isStandby());
        assertFalse(ClusterRole.ACTIVE.isStandby());
        assertFalse(ClusterRole.ACTIVE_NOT_IN_SYNC.isStandby());
        assertFalse(ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY.isStandby());
        assertFalse(ClusterRole.ACTIVE_TO_STANDBY.isStandby());
        assertFalse(ClusterRole.ACTIVE_WITH_OFFLINE_PEER.isStandby());
        assertFalse(ClusterRole.OFFLINE.isStandby());
        assertFalse(ClusterRole.UNKNOWN.isStandby());
    }

    @Test
    public void testClusterRoleIsMutationBlocked() {
        assertTrue(ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY.isMutationBlocked());
        assertTrue(ClusterRole.ACTIVE_TO_STANDBY.isMutationBlocked());

        assertFalse(ClusterRole.DEGRADED_STANDBY.isMutationBlocked());
        assertFalse(ClusterRole.DEGRADED_STANDBY_FOR_READER.isMutationBlocked());
        assertFalse(ClusterRole.DEGRADED_STANDBY_FOR_WRITER.isMutationBlocked());
        assertFalse(ClusterRole.STANDBY.isMutationBlocked());
        assertFalse(ClusterRole.STANDBY_TO_ACTIVE.isMutationBlocked());
        assertFalse(ClusterRole.ABORT_TO_ACTIVE.isMutationBlocked());
        assertFalse(ClusterRole.ABORT_TO_STANDBY.isMutationBlocked());
        assertFalse(ClusterRole.ACTIVE.isMutationBlocked());
        assertFalse(ClusterRole.ACTIVE_NOT_IN_SYNC.isMutationBlocked());
        assertFalse(ClusterRole.ACTIVE_WITH_OFFLINE_PEER.isMutationBlocked());
        assertFalse(ClusterRole.OFFLINE.isMutationBlocked());
        assertFalse(ClusterRole.UNKNOWN.isMutationBlocked());
    }

    @Test
    public void testClusterRoleFromBytes() {
        // Test valid role names
        assertEquals(ClusterRole.ACTIVE, ClusterRole.from("ACTIVE".getBytes()));
        assertEquals(ClusterRole.ACTIVE_TO_STANDBY, ClusterRole.from("ACTIVE_TO_STANDBY".getBytes()));
        assertEquals(ClusterRole.OFFLINE, ClusterRole.from("OFFLINE".getBytes()));
        assertEquals(ClusterRole.STANDBY, ClusterRole.from("STANDBY".getBytes()));
        assertEquals(ClusterRole.UNKNOWN, ClusterRole.from("UNKNOWN".getBytes()));

        // Test case insensitive
        assertEquals(ClusterRole.ACTIVE, ClusterRole.from("active".getBytes()));
        assertEquals(ClusterRole.ACTIVE_TO_STANDBY, ClusterRole.from("active_to_standby".getBytes()));
        assertEquals(ClusterRole.STANDBY, ClusterRole.from("standby".getBytes()));

        // Test mixed case
        assertEquals(ClusterRole.ACTIVE, ClusterRole.from("Active".getBytes()));
        assertEquals(ClusterRole.STANDBY, ClusterRole.from("StAnDbY".getBytes()));

        // Test invalid role name - should return UNKNOWN
        assertEquals(ClusterRole.UNKNOWN, ClusterRole.from("".getBytes()));
        assertEquals(ClusterRole.UNKNOWN, ClusterRole.from("INVALID_ROLE".getBytes()));
        assertEquals(ClusterRole.UNKNOWN, ClusterRole.from("null".getBytes()));
    }

    @Test
    public void testClusterRoleToString() {
        assertEquals("ABORT_TO_ACTIVE", ClusterRole.ABORT_TO_ACTIVE.toString());
        assertEquals("ABORT_TO_STANDBY", ClusterRole.ABORT_TO_STANDBY.toString());
        assertEquals("ACTIVE", ClusterRole.ACTIVE.toString());
        assertEquals("ACTIVE_NOT_IN_SYNC", ClusterRole.ACTIVE_NOT_IN_SYNC.toString());
        assertEquals("ACTIVE_NOT_IN_SYNC_TO_STANDBY", ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY.toString());
        assertEquals("ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER", ClusterRole.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER.toString());
        assertEquals("ACTIVE_TO_STANDBY", ClusterRole.ACTIVE_TO_STANDBY.toString());
        assertEquals("ACTIVE_WITH_OFFLINE_PEER", ClusterRole.ACTIVE_WITH_OFFLINE_PEER.toString());
        assertEquals("DEGRADED_STANDBY", ClusterRole.DEGRADED_STANDBY.toString());
        assertEquals("DEGRADED_STANDBY_FOR_READER", ClusterRole.DEGRADED_STANDBY_FOR_READER.toString());
        assertEquals("DEGRADED_STANDBY_FOR_WRITER", ClusterRole.DEGRADED_STANDBY_FOR_WRITER.toString());
        assertEquals("OFFLINE", ClusterRole.OFFLINE.toString());
        assertEquals("STANDBY", ClusterRole.STANDBY.toString());
        assertEquals("STANDBY_TO_ACTIVE", ClusterRole.STANDBY_TO_ACTIVE.toString());
        assertEquals("UNKNOWN", ClusterRole.UNKNOWN.toString());
    }

    @Test
    public void testClusterRoleEquality() {
        // Test enum equality
        assertNotEquals(ClusterRole.ACTIVE, ClusterRole.STANDBY);
        assertNotEquals(ClusterRole.ACTIVE, ClusterRole.UNKNOWN);

        // Test with valueOf
        assertEquals(ClusterRole.ACTIVE, ClusterRole.valueOf("ACTIVE"));
        assertEquals(ClusterRole.STANDBY, ClusterRole.valueOf("STANDBY"));
    }

    @Test
    public void testClusterRoleHashCode() {
        // Test that different enum values have different hash codes
        assertNotEquals(ClusterRole.ACTIVE.hashCode(), ClusterRole.STANDBY.hashCode());
        assertNotEquals(ClusterRole.ACTIVE.hashCode(), ClusterRole.OFFLINE.hashCode());
        assertNotEquals(ClusterRole.STANDBY.hashCode(), ClusterRole.UNKNOWN.hashCode());

        // Test that same enum values have same hash codes
        assertEquals(ClusterRole.ACTIVE.hashCode(), ClusterRole.valueOf("ACTIVE").hashCode());
        assertEquals(ClusterRole.STANDBY.hashCode(), ClusterRole.valueOf("STANDBY").hashCode());
    }

    @Test
    public void testClusterRoleCheckTransitionAndGetWaitTime() throws Exception {
        Configuration conf = new Configuration();
        conf.set(HA_SYNC_MODE_REFRESH_INTERVAL_MS, "3999");
        conf.set(HA_STORE_AND_FORWARD_MODE_REFRESH_INTERVAL_MS, "1999");

        // Test valid transitions with specific configuration values

        // Test ACTIVE_NOT_IN_SYNC transitions
        long waitTime = ClusterRole.ACTIVE_NOT_IN_SYNC.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE, conf);
        assertEquals("ACTIVE_NOT_IN_SYNC -> ACTIVE should return sync mode interval", 3999L, waitTime);

        waitTime = ClusterRole.ACTIVE_NOT_IN_SYNC.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE_NOT_IN_SYNC, conf);
        assertEquals("ACTIVE_NOT_IN_SYNC -> ACTIVE_NOT_IN_SYNC should return store and forward interval", 1999L, waitTime);

        waitTime = ClusterRole.ACTIVE_NOT_IN_SYNC.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE_NOT_IN_SYNC_TO_STANDBY, conf);
        assertEquals("ACTIVE_NOT_IN_SYNC -> ACTIVE_NOT_IN_SYNC_TO_STANDBY should return default", 0L, waitTime);

        // Test ACTIVE transitions
        waitTime = ClusterRole.ACTIVE.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE_NOT_IN_SYNC, conf);
        assertEquals("ACTIVE -> ACTIVE_NOT_IN_SYNC should return store and forward interval", 1999L, waitTime);

        waitTime = ClusterRole.ACTIVE.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE_WITH_OFFLINE_PEER, conf);
        assertEquals("ACTIVE -> ACTIVE_WITH_OFFLINE_PEER should return default", 0L, waitTime);

        waitTime = ClusterRole.ACTIVE.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE_TO_STANDBY, conf);
        assertEquals("ACTIVE -> ACTIVE_TO_STANDBY should return default", 0L, waitTime);

        // Test STANDBY transitions (should all return default 0L)
        waitTime = ClusterRole.STANDBY.checkTransitionAndGetWaitTime(ClusterRole.STANDBY_TO_ACTIVE, conf);
        assertEquals("STANDBY -> STANDBY_TO_ACTIVE should return default", 0L, waitTime);

        waitTime = ClusterRole.STANDBY.checkTransitionAndGetWaitTime(ClusterRole.DEGRADED_STANDBY_FOR_READER, conf);
        assertEquals("STANDBY -> DEGRADED_STANDBY_FOR_READER should return default", 0L, waitTime);

        waitTime = ClusterRole.STANDBY.checkTransitionAndGetWaitTime(ClusterRole.DEGRADED_STANDBY_FOR_WRITER, conf);
        assertEquals("STANDBY -> DEGRADED_STANDBY_FOR_WRITER should return default", 0L, waitTime);

        // Test other transitions that should return default 0L
        waitTime = ClusterRole.ACTIVE_TO_STANDBY.checkTransitionAndGetWaitTime(ClusterRole.ABORT_TO_ACTIVE, conf);
        assertEquals("ACTIVE_TO_STANDBY -> ABORT_TO_ACTIVE should return default", 0L, waitTime);

        waitTime = ClusterRole.STANDBY_TO_ACTIVE.checkTransitionAndGetWaitTime(ClusterRole.ABORT_TO_STANDBY, conf);
        assertEquals("STANDBY_TO_ACTIVE -> ABORT_TO_STANDBY should return default", 0L, waitTime);

        // Test invalid transitions should throw exception
        try {
            ClusterRole.ACTIVE.checkTransitionAndGetWaitTime(ClusterRole.STANDBY, conf);
            fail("Expected InvalidClusterRoleTransitionException");
        } catch (InvalidClusterRoleTransitionException e) {
            assertTrue("Exception message should contain transition info",
                    e.getMessage().contains("Cannot transition from ACTIVE to STANDBY"));
        }

        try {
            ClusterRole.STANDBY.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE, conf);
            fail("Expected InvalidClusterRoleTransitionException");
        } catch (InvalidClusterRoleTransitionException e) {
            assertTrue("Exception message should contain transition info",
                    e.getMessage().contains("Cannot transition from STANDBY to ACTIVE"));
        }

        try {
            ClusterRole.OFFLINE.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE, conf);
            fail("Expected InvalidClusterRoleTransitionException");
        } catch (InvalidClusterRoleTransitionException e) {
            assertTrue("Exception message should contain transition info",
                    e.getMessage().contains("Cannot transition from OFFLINE to ACTIVE"));
        }

        try {
            ClusterRole.UNKNOWN.checkTransitionAndGetWaitTime(ClusterRole.ACTIVE, conf);
            fail("Expected InvalidClusterRoleTransitionException");
        } catch (InvalidClusterRoleTransitionException e) {
            assertTrue("Exception message should contain transition info",
                    e.getMessage().contains("Cannot transition from UNKNOWN to ACTIVE"));
        }
    }

    //Private Helper Methods

    private ClusterRoleRecord getClusterRoleRecord(String name, HighAvailabilityPolicy policy,
                                                   String url1, ClusterRole role1, String url2, ClusterRole role2, int version) {
        url1 = getUrlWithSuffix(url1);
        url2 = getUrlWithSuffix(url2);
        if (registryType == null) {
            return new ClusterRoleRecord(
                    name, policy,
                    url1, role1,
                    url2, role2,
                    version);
        } else {
            return new ClusterRoleRecord(
                    name, policy,
                    registryType,
                    url1, role1,
                    url2, role2,
                    version);
        }
    }

    private String getUrlWithSuffix(String url) {
        if (registryType == null) {
            return url + "::/hbase";
        }
        switch (registryType){
            case MASTER:
            case RPC:
                return url;
            case ZK:
            default:
                return url + "::/hbase";
        }
    }

    private byte[] readFile(String fileName) throws IOException {
        InputStream inputStream = ClusterRoleRecordTest.class.getClassLoader().getResourceAsStream(fileName);
        assert inputStream != null;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] temp = new byte[1024];
        int bytesRead;
        while ((bytesRead = inputStream.read(temp)) != -1) {
            buffer.write(temp, 0, bytesRead);
        }
        return buffer.toByteArray();
    }
}