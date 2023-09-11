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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link ClusterRoleRecord}.
 */
public class ClusterRoleRecordTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecordTest.class);
    private static final String ZK1 = "zk1-1\\:2181,zk1-2\\:2181::/hbase";
    private static final String ZK2 = "zk2-1\\:2181,zk2-2\\:2181::/hbase";

    @Rule
    public final TestName testName = new TestName();

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

    @Test
    public void testReadWriteJsonToFile() throws IOException {
        ClusterRoleRecord record = new ClusterRoleRecord(
                testName.getMethodName(), HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2, ClusterRole.STANDBY,
                1);
        String fileName = createJsonFileWithRecords(record);
        String fileContent = FileUtils.readFileToString(new File(fileName), "UTF-8");
        assertTrue(fileContent.contains(record.getHaGroupName()));
    }

    @Test
    public void testToAndFromJson() throws IOException {
        ClusterRoleRecord record = new ClusterRoleRecord(
                testName.getMethodName(), HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2, ClusterRole.STANDBY,
                1);
        byte[] bytes = ClusterRoleRecord.toJson(record);
        Optional<ClusterRoleRecord> record2 = ClusterRoleRecord.fromJson(bytes);
        assertTrue(record2.isPresent());
        assertEquals(record, record2.get());
    }

    @Test
    public void testGetActiveUrl() {
        String haGroupName = testName.getMethodName();
        {
            ClusterRoleRecord record = new ClusterRoleRecord(
                    haGroupName, HighAvailabilityPolicy.FAILOVER,
                    ZK1, ClusterRole.ACTIVE,
                    ZK2, ClusterRole.STANDBY,
                    0);
            assertTrue(record.getActiveUrl().isPresent());
            assertEquals(ZK1, record.getActiveUrl().get());
        }
        {
            ClusterRoleRecord record = new ClusterRoleRecord(
                    haGroupName, HighAvailabilityPolicy.FAILOVER,
                    ZK1, ClusterRole.STANDBY,
                    ZK2, ClusterRole.STANDBY,
                    0);
            assertFalse(record.getActiveUrl().isPresent());
        }
    }

    @Test
    public void testIsNewerThan() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.STANDBY,
                ZK2 , ClusterRole.STANDBY,
                0);
        ClusterRoleRecord recordV1 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.STANDBY,
                ZK2 , ClusterRole.STANDBY,
                2);
        assertTrue(recordV1.isNewerThan(recordV0));  // v1 is indeed newer
        assertFalse(recordV1.isNewerThan(recordV1)); // irreflexive
        assertFalse(recordV0.isNewerThan(recordV1)); // antisymmetry

        // Create a new cluster role record for a new HA group name.
        // Cluster role records for different HA groups can not compare in reality,
        // so they are not newer than each other.
        String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                haGroupName2, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.STANDBY,
                ZK2 , ClusterRole.STANDBY,
                1);
        assertFalse(recordV0.isNewerThan(record2));
        assertFalse(recordV1.isNewerThan(record2));
        assertFalse(record2.isNewerThan(recordV0));
        assertFalse(record2.isNewerThan(recordV1));
    }

    @Test
    public void testHasSameInfo() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                0);
        ClusterRoleRecord recordV1 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                1);
        assertTrue(recordV1.hasSameInfo(recordV0));
        assertTrue(recordV1.hasSameInfo(recordV1));
        assertTrue(recordV0.hasSameInfo(recordV1));
    }

    @Test
    public void testHasSameInfoDifferentZKOrder() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK2, ClusterRole.ACTIVE,
                ZK1 , ClusterRole.STANDBY,
                0);
        ClusterRoleRecord recordV1 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                1);
        assertTrue(recordV1.hasSameInfo(recordV0));
        assertTrue(recordV1.hasSameInfo(recordV1));
        assertTrue(recordV0.hasSameInfo(recordV1));
    }

    @Test
    public void testHasSameInfoDifferentHostOrder() {
        String hostzk1ordered = "zk1-1,zk1-2:2181:/hbase";
        String hostzk1unordered = "zk1-2,zk1-1:2181:/hbase";
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord recordV0 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK2, ClusterRole.ACTIVE,
                hostzk1ordered , ClusterRole.STANDBY,
                0);
        ClusterRoleRecord recordV1 = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                hostzk1unordered, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                1);
        assertTrue(recordV1.hasSameInfo(recordV0));
        assertTrue(recordV1.hasSameInfo(recordV1));
        assertTrue(recordV0.hasSameInfo(recordV1));
    }

    @Test
    public void testHasSameInfoNegative() {
        String haGroupName = testName.getMethodName();
        ClusterRoleRecord record = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.PARALLEL,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                0);

        ClusterRoleRecord recordFailover = new ClusterRoleRecord(
                haGroupName, HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                1);
        assertFalse(record.hasSameInfo(recordFailover));
        assertFalse(recordFailover.hasSameInfo(record));

        String haGroupName2 = haGroupName + RandomStringUtils.randomAlphabetic(2);
        ClusterRoleRecord record2 = new ClusterRoleRecord(
                haGroupName2, HighAvailabilityPolicy.PARALLEL,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                1);
        assertFalse(record.hasSameInfo(record2));
        assertFalse(record2.hasSameInfo(record));
    }

    @Test
    public void testGetRole() {
        ClusterRoleRecord record = new ClusterRoleRecord(
                testName.getMethodName(), HighAvailabilityPolicy.FAILOVER,
                ZK1, ClusterRole.ACTIVE,
                ZK2 , ClusterRole.STANDBY,
                0);
        assertEquals(ClusterRole.ACTIVE, record.getRole(ZK1));
        assertEquals(ClusterRole.ACTIVE, record.getRole(record.getZk1()));
        assertEquals(ClusterRole.STANDBY, record.getRole(record.getZk2()));
        assertEquals(ClusterRole.UNKNOWN, record.getRole(null));
        assertEquals(ClusterRole.UNKNOWN, record.getRole("foo"));
    }

    @Test
    public void testToPrettyString() {
        ClusterRoleRecord record = new ClusterRoleRecord(
                testName.getMethodName(), HighAvailabilityPolicy.PARALLEL,
                ZK1, ClusterRole.ACTIVE,
                ZK2, ClusterRole.STANDBY,
                1);
        LOG.info("toString(): {}", record.toString());
        LOG.info("toPrettyString:\n{}", record.toPrettyString());
        assertNotEquals(record.toString(), record.toPrettyString());
    }
}
