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
package org.apache.phoenix.end2end.salted;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Tests for table with transparent salting.
 */

@Category(ParallelStatsDisabledTest.class)
public class SaltedTableIT extends BaseSaltedTableIT {

    @Test
    public void testTableWithInvalidBucketNumber() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String query = "create table " + generateUniqueName() + " (a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS = 257";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            fail("Should have caught exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1021 (42Y80): Salt bucket numbers should be with 1 and 256."));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableWithExplicitSplit() throws Exception {
        byte[][] expectedStartKeys =
        new byte[][] { {},
            {1, 0, 0, 0, 0},
            {2, 0, 0, 0, 0},
            {2, 3, 0, 0, 0},
            {2, 5, 0, 0, 0},
            {3, 0, 0, 0, 0}};
        String tableName = generateUniqueName();
        createTestTable(getUrl(), "create table " + tableName + " (a_integer integer not null primary key) SALT_BUCKETS = 4",
                new byte[][] {{1}, {2,3}, {2,5}, {3}}, null);
        Admin admin = utility.getAdmin();
        ArrayList<byte[]> startKeys = new ArrayList<>();
        List<RegionInfo> regionInfos =
                admin.getRegions(TableName.valueOf(tableName));
        for (RegionInfo regionInfo : regionInfos) {
            startKeys.add(regionInfo.getStartKey());
        }
        assertTrue(Arrays.deepEquals(expectedStartKeys, startKeys.toArray()));
    }

    @Test
    public void testExistingTableWithBadSplit() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        try (
                Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();)
        {
            String tableNameString =  generateUniqueName();
            stmt.execute("create table " + tableNameString + " (a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS = 4");
            //leaves HBase table intact
            stmt.execute("drop table " + tableNameString);
            stmt.execute("create table " + tableNameString + " (a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS = 5");
            fail("Should have caught exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1148 (42Y91): The existing HBase table is not a Phoenix salted table with the specified number of buckets."));
        }
    }
}
