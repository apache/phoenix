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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.util.JacksonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link HAGroupStoreRecord}.
 */
public class HAGroupStoreRecordTest {
    private static final Logger LOG = LoggerFactory.getLogger(HAGroupStoreRecordTest.class);
    private static final String PROTOCOL_VERSION = "1.0";
    private static final String POLICY = "FAILOVER";

    @Rule
    public final TestName testName = new TestName();

    /**
     * Helper method to create a temp JSON file with the given array of HA group store records.
     */
    public static String createJsonFileWithRecords(HAGroupStoreRecord record)
            throws IOException {
        File file = File.createTempFile("phoenix.ha.group.store.records", ".test.json");
        file.deleteOnExit();
        JacksonUtil.getObjectWriterPretty().writeValue(file, record);
        LOG.info("Prepared the JSON file for testing, file:{}, content:\n{}", file,
                FileUtils.readFileToString(file, "UTF-8"));
        return file.getPath();
    }

    @Test
    public void testReadWriteJsonToFile() throws IOException {
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), "peer1:2181,peer2:2181");
        String fileName = createJsonFileWithRecords(record);
        String fileContent = FileUtils.readFileToString(new File(fileName), "UTF-8");
        assertTrue(fileContent.contains(record.getHaGroupName()));
        assertTrue(fileContent.contains(record.getProtocolVersion()));
        assertTrue(fileContent.contains(record.getPolicy()));
    }

    @Test
    public void testToAndFromJson() throws IOException {
        long currentTime = System.currentTimeMillis();
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, "backup1:2181,backup2:2181");
        byte[] bytes = HAGroupStoreRecord.toJson(record);
        Optional<HAGroupStoreRecord> record2 = HAGroupStoreRecord.fromJson(bytes);
        assertTrue(record2.isPresent());
        assertEquals(record, record2.get());
    }

    @Test
    public void testFromJsonWithNullBytes() {
        Optional<HAGroupStoreRecord> record = HAGroupStoreRecord.fromJson(null);
        assertFalse(record.isPresent());
    }

    @Test
    public void testFromJsonWithInvalidJson() {
        byte[] invalidJson = "invalid json".getBytes();
        Optional<HAGroupStoreRecord> record = HAGroupStoreRecord.fromJson(invalidJson);
        assertFalse(record.isPresent());
    }

    @Test
    public void testIsNewerThan() {
        String haGroupName = testName.getMethodName();
        long currentTime = System.currentTimeMillis();
        HAGroupStoreRecord recordV0 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 0, POLICY, currentTime, "primary1:2181,primary2:2181");
        HAGroupStoreRecord recordV2 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 2, POLICY, currentTime, "primary1:2181,primary2:2181");

        assertTrue(recordV2.isNewerThan(recordV0));  // v2 is indeed newer
        assertFalse(recordV2.isNewerThan(recordV2)); // irreflexive
        assertFalse(recordV0.isNewerThan(recordV2)); // antisymmetry
        assertTrue(recordV0.isNewerThan(null));      // null comparison

        // Create a new record for a different HA group name.
        // Records for different HA groups can not compare in reality,
        // so they are not newer than each other.
        String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
        HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName2,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, "secondary1:2181,secondary2:2181");
        assertFalse(recordV0.isNewerThan(record2));
        assertFalse(recordV2.isNewerThan(record2));
        assertFalse(record2.isNewerThan(recordV0));
        assertFalse(record2.isNewerThan(recordV2));
    }

    @Test
    public void testHasSameInfo() {
        String haGroupName = testName.getMethodName();
        long currentTime = System.currentTimeMillis();
        HAGroupStoreRecord recordV0 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 0, POLICY, currentTime, "cluster1:2181,cluster2:2181");
        HAGroupStoreRecord recordV1 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.STANDBY, 1, POLICY, currentTime + 1000, "cluster1:2181,cluster2:2181");

        assertTrue(recordV1.hasSameInfo(recordV0)); // Same core info despite different role/version/time
        assertTrue(recordV1.hasSameInfo(recordV1)); // reflexive
        assertTrue(recordV0.hasSameInfo(recordV1)); // symmetric
    }

    @Test
    public void testHasSameInfoNegative() {
        String haGroupName = testName.getMethodName();
        long currentTime = System.currentTimeMillis();
        HAGroupStoreRecord record = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 0, POLICY, currentTime, "base1:2181,base2:2181");

        // Different protocol version
        HAGroupStoreRecord recordDifferentProtocol = getHAGroupStoreRecord(haGroupName,
                "2.0", ClusterRole.ACTIVE, 1, POLICY, currentTime, "protocol1:2181,protocol2:2181");
        assertFalse(record.hasSameInfo(recordDifferentProtocol));
        assertFalse(recordDifferentProtocol.hasSameInfo(record));

        // Different policy
        HAGroupStoreRecord recordDifferentPolicy = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, "PARALLEL", currentTime, "policy1:2181,policy2:2181");
        assertFalse(record.hasSameInfo(recordDifferentPolicy));
        assertFalse(recordDifferentPolicy.hasSameInfo(record));

        // Different HA group name
        String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
        HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName2,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, "group1:2181,group2:2181");
        assertFalse(record.hasSameInfo(record2));
        assertFalse(record2.hasSameInfo(record));
    }

    @Test
    public void testGetters() {
        String haGroupName = testName.getMethodName();
        String protocolVersion = "1.5";
        ClusterRole clusterRole = ClusterRole.STANDBY;
        int version = 42;
        String policy = "CUSTOM_POLICY";
        long lastUpdatedTime = System.currentTimeMillis();

        HAGroupStoreRecord record = getHAGroupStoreRecord(haGroupName, protocolVersion,
                clusterRole, version, policy, lastUpdatedTime, null);

        assertEquals(haGroupName, record.getHaGroupName());
        assertEquals(protocolVersion, record.getProtocolVersion());
        assertEquals(clusterRole, record.getClusterRole());
        assertEquals(version, record.getVersion());
        assertEquals(policy, record.getPolicy());
        assertEquals(lastUpdatedTime, record.getLastUpdatedTimeInMs());
        assertNull(record.getPeerZKUrl()); // Default should be null
    }

    @Test
    public void testEqualsAndHashCode() {
        String haGroupName = testName.getMethodName();
        long currentTime = System.currentTimeMillis();
        HAGroupStoreRecord record1 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, "equal1:2181,equal2:2181");
        HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, "equal1:2181,equal2:2181");
        HAGroupStoreRecord record3 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 2, POLICY, currentTime, "equal1:2181,equal2:2181"); // Different version

        // Test equals
        assertEquals(record1, record2); // symmetric
        assertEquals(record2, record1); // symmetric
        assertNotEquals(record1, record3); // different version
        assertNotEquals(null, record1); // null comparison
        assertNotEquals("not a record", record1); // different type

        // Test hashCode
        assertEquals(record1.hashCode(), record2.hashCode()); // equal objects have same hash
        assertNotEquals(record1.hashCode(), record3.hashCode()); // different objects likely have different hash
    }

    @Test
    public void testToString() {
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), "tostring1:2181,tostring2:2181");
        String toString = record.toString();

        // Verify all fields are present in toString
        assertTrue(toString.contains(record.getHaGroupName()));
        assertTrue(toString.contains(record.getProtocolVersion()));
        assertTrue(toString.contains(record.getClusterRole().toString()));
        assertTrue(toString.contains(String.valueOf(record.getVersion())));
        assertTrue(toString.contains(record.getPolicy()));
        assertTrue(toString.contains(String.valueOf(record.getLastUpdatedTimeInMs())));
    }

    @Test
    public void testToPrettyString() {
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), "pretty1:2181,pretty2:2181");
        LOG.info("toString(): {}", record.toString());
        LOG.info("toPrettyString:\n{}", record.toPrettyString());
        assertNotEquals(record.toString(), record.toPrettyString());
        assertTrue(record.toPrettyString().contains(record.getHaGroupName()));
    }

    @Test
    public void testPeerZKUrlWithNullValue() {
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), null);
        assertNull(record.getPeerZKUrl());

        // Test toString includes peerZKUrl field
        String toString = record.toString();
        assertTrue(toString.contains("peerZKUrl"));
        assertTrue(toString.contains("null"));
    }

    @Test
    public void testPeerZKUrlWithValidValue() {
        String peerZKUrl = "peer1:2181,peer2:2181,peer3:2181";
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), peerZKUrl);
        assertEquals(peerZKUrl, record.getPeerZKUrl());

        // Test toString includes peerZKUrl field
        String toString = record.toString();
        assertTrue(toString.contains("peerZKUrl"));
        assertTrue(toString.contains(peerZKUrl));
    }

    @Test
    public void testJsonSerializationWithPeerZKUrl() throws IOException {
        String peerZKUrl = "peer1:2181,peer2:2181,peer3:2181";
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), peerZKUrl);

        // Test serialization and deserialization
        byte[] bytes = HAGroupStoreRecord.toJson(record);
        Optional<HAGroupStoreRecord> deserialized = HAGroupStoreRecord.fromJson(bytes);

        assertTrue(deserialized.isPresent());
        assertEquals(record, deserialized.get());
        assertEquals(peerZKUrl, deserialized.get().getPeerZKUrl());
    }

    @Test
    public void testJsonSerializationWithNullPeerZKUrl() throws IOException {
        HAGroupStoreRecord record = getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), null);

        // Test serialization and deserialization
        byte[] bytes = HAGroupStoreRecord.toJson(record);
        Optional<HAGroupStoreRecord> deserialized = HAGroupStoreRecord.fromJson(bytes);

        assertTrue(deserialized.isPresent());
        assertEquals(record, deserialized.get());
        assertNull(deserialized.get().getPeerZKUrl());
    }

    @Test
    public void testEqualsAndHashCodeWithPeerZKUrl() {
        String haGroupName = testName.getMethodName();
        long currentTime = System.currentTimeMillis();
        String peerZKUrl = "peer1:2181,peer2:2181";

        HAGroupStoreRecord record1 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, peerZKUrl);
        HAGroupStoreRecord record2 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, peerZKUrl);
        HAGroupStoreRecord record3 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, "different:2181");
        HAGroupStoreRecord record4 = getHAGroupStoreRecord(haGroupName,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, currentTime, null);

        // Test equals with same peerZKUrl
        assertEquals(record1, record2);
        assertEquals(record2, record1);
        assertEquals(record1.hashCode(), record2.hashCode());

        // Test equals with different peerZKUrl
        assertNotEquals(record1, record3);
        assertNotEquals(record3, record1);
        assertNotEquals(record1.hashCode(), record3.hashCode());

        // Test equals with null vs non-null peerZKUrl
        assertNotEquals(record1, record4);
        assertNotEquals(record4, record1);
        assertNotEquals(record1.hashCode(), record4.hashCode());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullHaGroupName() {
        getHAGroupStoreRecord(null,
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, POLICY, System.currentTimeMillis(), "peer:2181");
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullClusterRole() {
        getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, null, 1, POLICY, System.currentTimeMillis(), "peer:2181");
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullPolicy() {
        getHAGroupStoreRecord(testName.getMethodName(),
                PROTOCOL_VERSION, ClusterRole.ACTIVE, 1, null, System.currentTimeMillis(), "peer:2181");
    }

    // Private Helper Methods
    private HAGroupStoreRecord getHAGroupStoreRecord(String haGroupName, String protocolVersion,
                                                     ClusterRole clusterRole, int version,
                                                     String policy, long lastUpdatedTimeInMs,
                                                     String peerZKUrl) {
        return new HAGroupStoreRecord(protocolVersion, haGroupName, clusterRole,
                version, policy, lastUpdatedTimeInMs, peerZKUrl);
    }
}