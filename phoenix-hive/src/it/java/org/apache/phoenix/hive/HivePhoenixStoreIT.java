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
package org.apache.phoenix.hive;

import org.apache.hadoop.fs.Path;
import org.apache.phoenix.util.StringUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertTrue;

/**
 * Test methods only. All supporting methods should be placed to BaseHivePhoenixStoreIT
 */

@Ignore("This class contains only test methods and should not be executed directly")
public class HivePhoenixStoreIT  extends BaseHivePhoenixStoreIT {

    /**
     * Create a table with two column, insert 1 row, check that phoenix table is created and
     * the row is there
     *
     * @throws Exception
     */
    @Test
    public void simpleTest() throws Exception {
        String testName = "simpleTest";
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE phoenix_table(ID STRING, SALARY STRING)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='phoenix_table'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE phoenix_table" + HiveTestUtil.CRLF +
                "VALUES ('10', '1000');" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_table";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 2);
        assertTrue(rs.next());
        assert (rs.getString(1).equals("10"));
        assert (rs.getString(2).equals("1000"));
    }

    /**
     * Create hive table with custom column mapping
     * @throws Exception
     */

    @Test
    public void simpleColumnMapTest() throws Exception {
        String testName = "cmTest";
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE column_table(ID STRING, P1 STRING, p2 STRING)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.table.name'='column_table'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:C1, p1:c2, p2:C3'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE column_table" + HiveTestUtil.CRLF +
                "VALUES ('1', '2', '3');" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT C1, \"c2\", C3 FROM column_table";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 3);
        assertTrue(rs.next());
        assert (rs.getString(1).equals("1"));
        assert (rs.getString(2).equals("2"));
        assert (rs.getString(3).equals("3"));

    }


    /**
     * Datatype Test
     *
     * @throws Exception
     */
    @Test
    public void dataTypeTest() throws Exception {
        String testName = "dataTypeTest";
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE phoenix_datatype(ID int, description STRING, ts TIMESTAMP,  db " +
                "DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF + " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='phoenix_datatype'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id');");
        sb.append("INSERT INTO TABLE phoenix_datatype" + HiveTestUtil.CRLF +
                "VALUES (10, \"foodesc\", \"2013-01-05 01:01:01\", 200,2.0,-1);" + HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_datatype";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 6);
        while (rs.next()) {
            assert (rs.getInt(1) == 10);
            assert (rs.getString(2).equalsIgnoreCase("foodesc"));
            assert (rs.getDouble(4) == 200);
            assert (rs.getFloat(5) == 2.0);
            assert (rs.getInt(6) == -1);
        }
    }

    /**
     * Datatype Test
     *
     * @throws Exception
     */
    @Test
    public void MultiKey() throws Exception {
        String testName = "MultiKey";
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE phoenix_MultiKey(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='phoenix_MultiKey'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE phoenix_MultiKey VALUES (10, \"part2\",\"foodesc\",200,2.0,-1);" +
                HiveTestUtil.CRLF);
        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);

        String phoenixQuery = "SELECT * FROM phoenix_MultiKey";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 6);
        while (rs.next()) {
            assert (rs.getInt(1) == 10);
            assert (rs.getString(2).equalsIgnoreCase("part2"));
            assert (rs.getString(3).equalsIgnoreCase("foodesc"));
            assert (rs.getDouble(4) == 200);
            assert (rs.getFloat(5) == 2.0);
            assert (rs.getInt(6) == -1);
        }
    }

    /**
     * Test that hive is able to access Phoenix data during MR job (creating two tables and perform join on it)
     *
     * @throws Exception
     */
    @Test
    public void testJoinNoColumnMaps() throws Exception {
        String testName = "testJoin";
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());
        createFile("10\tpart2\tfoodesc\t200.0\t2.0\t-1\t10\tpart2\tfoodesc\t200.0\t2.0\t-1\n",
                new Path(hiveOutputDir, testName + ".out").toString());
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE joinTable1(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='joinTable1'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);
        sb.append("CREATE TABLE joinTable2(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='joinTable2'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable1 VALUES (5, \"part2\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable1 VALUES (10, \"part2\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable2 VALUES (5, \"part2\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable2 VALUES (10, \"part2\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("SELECT  * from joinTable1 A join joinTable2 B on A.ID = B.ID WHERE A.ID=10;" +
                HiveTestUtil.CRLF);

        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
    }

    /**
     * Test that hive is able to access Phoenix data during MR job (creating two tables and perform join on it)
     *
     * @throws Exception
     */
    @Test
    public void testJoinColumnMaps() throws Exception {
        String testName = "testJoin";
        hbaseTestUtil.getTestFileSystem().createNewFile(new Path(hiveLogDir, testName + ".out"));
        createFile("10\t200.0\tpart2\n", new Path(hiveOutputDir, testName + ".out").toString());
        createFile(StringUtil.EMPTY_STRING, new Path(hiveLogDir, testName + ".out").toString());

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE joinTable3(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='joinTable3'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:i1, id2:I2'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);
        sb.append("CREATE TABLE joinTable4(ID int, ID2 String,description STRING," +
                "db DOUBLE,fl FLOAT, us INT)" + HiveTestUtil.CRLF +
                " STORED BY  \"org.apache.phoenix.hive.PhoenixStorageHandler\"" + HiveTestUtil
                .CRLF +
                " TBLPROPERTIES(" + HiveTestUtil.CRLF +
                "   'phoenix.hbase.table.name'='joinTable4'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.znode.parent'='/hbase'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.quorum'='localhost'," + HiveTestUtil.CRLF +
                "   'phoenix.zookeeper.client.port'='" +
                hbaseTestUtil.getZkCluster().getClientPort() + "'," + HiveTestUtil.CRLF +
                "   'phoenix.column.mapping' = 'id:i1, id2:I2'," + HiveTestUtil.CRLF +
                "   'phoenix.rowkeys'='id,id2');" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable3 VALUES (5, \"part1\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable3 VALUES (10, \"part1\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("INSERT INTO TABLE joinTable4 VALUES (5, \"part2\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);
        sb.append("INSERT INTO TABLE joinTable4 VALUES (10, \"part2\",\"foodesc\",200,2.0,-1);" + HiveTestUtil.CRLF);

        sb.append("SELECT A.ID, a.db, B.ID2 from joinTable3 A join joinTable4 B on A.ID = B.ID WHERE A.ID=10;" +
                HiveTestUtil.CRLF);

        String fullPath = new Path(hbaseTestUtil.getDataTestDir(), testName).toString();
        createFile(sb.toString(), fullPath);
        runTest(testName, fullPath);
        //Test that Phoenix has correctly mapped columns. We are checking both, primary key and
        // regular columns mapped and not mapped
        String phoenixQuery = "SELECT \"i1\", \"I2\", \"db\" FROM joinTable3 where \"i1\" = 10 AND \"I2\" = 'part1' AND \"db\" = 200";
        PreparedStatement statement = conn.prepareStatement(phoenixQuery);
        ResultSet rs = statement.executeQuery();
        assert (rs.getMetaData().getColumnCount() == 3);
        while (rs.next()) {
            assert (rs.getInt(1) == 10);
            assert (rs.getString(2).equalsIgnoreCase("part1"));
            assert (rs.getDouble(3) == 200);
        }

    }
}
