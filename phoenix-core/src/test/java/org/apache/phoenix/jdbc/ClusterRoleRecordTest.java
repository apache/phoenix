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

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
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

/**
 * Unit test for {@link ClusterRoleRecord}.
 */
public class ClusterRoleRecordTest {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRoleRecordTest.class);
    private static final String URL1 = "zk1-1\\:2181,zk1-2\\:2181";
    private static final String URL2 = "zk2-1\\:2181,zk2-2\\:2181";
    private final ClusterRoleRecord.RegistryType registryType = DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE;

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

    @Test
    public void testClusterRoleFromInvalidBytes() {
        ClusterRole role = ClusterRole.from(new byte[0]);
        assertEquals(ClusterRole.UNKNOWN, role);

        role = ClusterRole.from("random".getBytes());
        assertEquals(ClusterRole.UNKNOWN, role);
    }

    //Private Helper Methods

    private ClusterRoleRecord getClusterRoleRecord(String name, HighAvailabilityPolicy policy,
                              String url1, ClusterRole role1, String url2, ClusterRole role2, int version) {
        url1 = getUrlWithSuffix(url1);
        url2 = getUrlWithSuffix(url2);
        return new ClusterRoleRecord(
                name, policy,
                url1, role1,
                url2, role2,
                version);
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