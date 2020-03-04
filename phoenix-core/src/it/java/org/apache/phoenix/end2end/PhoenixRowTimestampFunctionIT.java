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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PhoenixRowTimestampFunctionIT extends ParallelStatsDisabledIT {
    private final boolean encoded;
    private final String tableDDLOptions;

    public PhoenixRowTimestampFunctionIT(QualifierEncodingScheme encoding,
            ImmutableStorageScheme storage) {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" COLUMN_ENCODED_BYTES = " + encoding.ordinal());
        optionBuilder.append(",IMMUTABLE_STORAGE_SCHEME = "+ storage.toString());
        this.tableDDLOptions = optionBuilder.toString();
        this.encoded = (encoding != QualifierEncodingScheme.NON_ENCODED_QUALIFIERS) ? true : false;
    }

    @Parameterized.Parameters(name = "encoding={0},storage={1}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayList();
        for (QualifierEncodingScheme encoding : QualifierEncodingScheme.values()) {
            for (ImmutableStorageScheme storage : ImmutableStorageScheme.values()) {
                list.add(new Object[]{encoding, storage});
            }
        }
        return list;
    }

    private void verifyHbaseAllRowsTimestamp(String tableName, ResultSet rs, int expectedRowCount)
            throws Exception {

        Scan scan = new Scan();
        byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(this.encoded).getFirst();
        try (org.apache.hadoop.hbase.client.Connection hconn =
                ConnectionFactory.createConnection(config)) {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = table.getScanner(scan);
            int rowCount = 0;
            while (rs.next()) {
                Result result = resultScanner.next();
                long timeStamp = result.getColumnLatestCell(
                        QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                        emptyKVQualifier).getTimestamp();
                assertEquals(rs.getDate(1).getTime(), timeStamp);
                ++rowCount;
            }
            assertEquals(expectedRowCount, rowCount);
        }
    }

    private void verifyHbaseRowTimestamp(String tableName, String rowKey, Date expectedTimestamp)
            throws Exception {

        byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(this.encoded).getFirst();
        try (org.apache.hadoop.hbase.client.Connection hconn =
                ConnectionFactory.createConnection(config)) {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytesBinary(rowKey));
            Result result = table.get(get);
            assertFalse(result.isEmpty());
            long timeStamp = result.getColumnLatestCell(
                    QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, emptyKVQualifier).getTimestamp();
            assertEquals(expectedTimestamp.getTime(), timeStamp);
        }
    }

    @Test
    public void testRowTimestampDefault() throws Exception {

        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName
                    + " (PK INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR, KV2 VARCHAR)"
                    + this.tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + tableName + " (PK, KV1, KV2) VALUES (?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);

            int count = 5;
            for (int id = 0; id < count; ++id) {
                stmt.setInt(1, id);
                stmt.setString(2, "KV1_" + id);
                stmt.setString(3, "KV2_" + id);
                stmt.executeUpdate();
            }
            conn.commit();

            String dql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName;

            ResultSet rs = conn.createStatement().executeQuery(dql);
            // verify row timestamp returned by the query matches the empty column cell timestamp
            verifyHbaseAllRowsTimestamp(tableName, rs, count);

            // update one row
            conn.createStatement().execute("UPSERT INTO " + tableName
                    + " (PK, KV1) VALUES (2, 'KV1_foo')");
            conn.commit();

            rs = conn.createStatement().executeQuery(dql);
            // verify again after update
            verifyHbaseAllRowsTimestamp(tableName, rs, count);

            dql = "SELECT ROWKEY_BYTES_STRING(), PHOENIX_ROW_TIMESTAMP() FROM " + tableName
                    + " WHERE PK >= 1 AND PK <=3 ";
            rs = conn.createStatement().executeQuery(dql);

            while (rs.next()) {
                verifyHbaseRowTimestamp(tableName, rs.getString(1), rs.getDate(2));
            }
        }
    }

    @Test
    public void testRowTimestampColumn() throws Exception {
        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName
                    + " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR"
                    + " CONSTRAINT PK PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP))" + this.tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + tableName + " (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)";

            long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            Date rowTimestampDate = new Date(rowTimestamp);
            PreparedStatement stmt = conn.prepareStatement(dml);

            int count = 5;
            for (int id = 0; id < count; ++id) {
                stmt.setInt(1, id);
                stmt.setDate(2, rowTimestampDate);
                stmt.setString(3, "KV1_" + id);
                stmt.setString(4, "KV2_" + id);
                stmt.executeUpdate();
            }
            conn.commit();

            String dql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName;

            ResultSet rs = conn.createStatement().executeQuery(dql);
            while(rs.next()) {
                assertEquals(rs.getDate(1), rowTimestampDate);
            }
        }
    }

    @Test
    public void testRowTimestampColumnAndPredicates() throws Exception {
        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName
                    + " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR"
                    + " CONSTRAINT PK PRIMARY KEY(PK1, PK2))" + this.tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + tableName + " (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)";

            long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            Date rowTimestampDate1 = new Date(rowTimestamp - 120000);
            PreparedStatement stmt = conn.prepareStatement(dml);

            int count = 5;
            for (int id = 0; id < count; ++id) {
                int idValue = id;
                stmt.setInt(1, idValue);
                stmt.setDate(2, rowTimestampDate1);
                stmt.setString(3, "KV1_" + idValue);
                stmt.setString(4, "KV2_" + idValue);
                stmt.executeUpdate();
            }
            conn.commit();

            // case: No rows should be selected
            // Since we used a past date for column PK2
            String dql1 = "SELECT PHOENIX_ROW_TIMESTAMP(), KV1 FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() = PK2";

            ResultSet rs1 = conn.createStatement().executeQuery(dql1);
            assertTrue(!rs1.next());
            rs1.close();

            // case: All rows selected should have the phoenix_row_timestamp() > date column
            // Since we used a past date for column PK2
            String dql2 = "SELECT PHOENIX_ROW_TIMESTAMP(), KV1 FROM " + tableName +
                    " WHERE PK2 < PHOENIX_ROW_TIMESTAMP() ";

            ResultSet rs2 = conn.createStatement().executeQuery(dql2);
            int actualCount2 = 0;
            while(rs2.next()) {
                assertTrue(rs2.getDate(1).after(rowTimestampDate1));
                actualCount2++;
            }
            assertEquals(count, actualCount2);
            rs2.close();

            // case: All rows selected should have the phoenix_row_timestamp() < date column
            // So using a future date.
            Date rowTimestampDate2 = new Date(rowTimestamp + 120000);
            for (int id = 0; id < count; ++id) {
                int idValue = id + 100;
                stmt.setInt(1, idValue);
                stmt.setDate(2, rowTimestampDate2);
                stmt.setString(3, "KV1_" + idValue);
                stmt.setString(4, "KV2_" + idValue);
                stmt.executeUpdate();
            }
            conn.commit();

            String dql3 = "SELECT PHOENIX_ROW_TIMESTAMP(), KV2 FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() < PK2";

            ResultSet rs3 = conn.createStatement().executeQuery(dql3);
            int actualCount3 = 0;
            while(rs3.next()) {
                assertTrue(rs3.getDate(1).before(rowTimestampDate2));
                actualCount3++;
            }
            assertEquals(count, actualCount3);
            rs3.close();
        }
    }

    @Ignore("Fails when encoding=NON_ENCODED_QUALIFIERS and PHOENIX_ROW_TIMESTAMP "
            + "is the only select column"
            + "Expected rows do match expected:<5> but was:<0>")
    public void testRowTimestampColumnOnlyAndPredicates() throws Exception {
        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName
                    + " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR"
                    + " CONSTRAINT PK PRIMARY KEY(PK1, PK2))" + this.tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + tableName + " (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)";

            long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            Date rowTimestampDate1 = new Date(rowTimestamp - 120000);
            PreparedStatement stmt = conn.prepareStatement(dml);

            int count = 5;
            for (int id = 0; id < count; ++id) {
                int idValue = id;
                stmt.setInt(1, idValue);
                stmt.setDate(2, rowTimestampDate1);
                stmt.setString(3, "KV1_" + idValue);
                stmt.setString(4, "KV2_" + idValue);
                stmt.executeUpdate();
            }
            conn.commit();

            // case: No rows should be selected
            // Since we used a past date for column PK2
            String dql1 = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() = PK2";

            ResultSet rs1 = conn.createStatement().executeQuery(dql1);
            assertTrue(!rs1.next());
            rs1.close();

            // case: All rows selected should have the phoenix_row_timestamp() > date column
            // Since we used a past date for column PK2
            String dql2 = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName +
                    " WHERE PK2 < PHOENIX_ROW_TIMESTAMP() ";

            ResultSet rs2 = conn.createStatement().executeQuery(dql2);
            int actualCount2 = 0;
            while(rs2.next()) {
                assertTrue(rs2.getDate(1).after(rowTimestampDate1));
                actualCount2++;
            }
            assertEquals(count, actualCount2);
            rs2.close();

            // case: All rows selected should have the phoenix_row_timestamp() < date column
            // So using a future date.
            Date rowTimestampDate2 = new Date(rowTimestamp + 120000);
            for (int id = 0; id < count; ++id) {
                int idValue = id + 100;
                stmt.setInt(1, idValue);
                stmt.setDate(2, rowTimestampDate2);
                stmt.setString(3, "KV1_" + idValue);
                stmt.setString(4, "KV2_" + idValue);
                stmt.executeUpdate();
            }
            conn.commit();

            String dql3 = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() < PK2";

            ResultSet rs3 = conn.createStatement().executeQuery(dql3);
            int actualCount3 = 0;
            while(rs3.next()) {
                assertTrue(rs3.getDate(1).before(rowTimestampDate2));
                actualCount3++;
            }
            assertEquals(count, actualCount3);
            rs3.close();
        }
    }

}
