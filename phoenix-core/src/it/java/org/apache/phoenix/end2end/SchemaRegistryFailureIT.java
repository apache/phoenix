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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.export.SchemaRegistryRepository;
import org.apache.phoenix.schema.export.SchemaWriter;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY;

@Category(NeedsOwnMiniClusterTest.class)
public class SchemaRegistryFailureIT extends ParallelStatsDisabledIT{

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(SchemaRegistryRepository.SCHEMA_REGISTRY_IMPL_KEY,
            ExplodingSchemaRegistryRepository.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testFailedCreateRollback() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL," +
                " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) "
                + "MULTI_TENANT=true, CHANGE_DETECTION_ENABLED=true";
            try {
                conn.createStatement().execute(ddl);
                Assert.fail("Should have thrown SQLException");
            } catch (SQLException e) {
                Assert.assertEquals(SQLExceptionCode.ERROR_WRITING_TO_SCHEMA_REGISTRY.getErrorCode(),
                    e.getErrorCode());
            }

            try {
                PTable table = conn.getTable(fullTableName);
                Assert.fail("Shouldn't have found the table because it shouldn't have been created");
            } catch (TableNotFoundException tnfe) {
                //eat the exception, which is what we expect
            }
        }
    }

    @Test
    public void testFailedAlterRollback() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE " + fullTableName + " (id char(1) NOT NULL,"
                + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) " + "MULTI_TENANT=true";

            conn.createStatement().execute(ddl);

            String alterDdl = "ALTER TABLE " + fullTableName + " SET CHANGE_DETECTION_ENABLED=true";
            try {
                conn.createStatement().execute(alterDdl);
                Assert.fail("Should have failed because of schema registry exception");
            } catch (SQLException se) {
                Assert.assertEquals(SQLExceptionCode.ERROR_WRITING_TO_SCHEMA_REGISTRY.getErrorCode(),
                    se.getErrorCode());
            }
            PTable table = conn.getTable(fullTableName);
            Assert.assertFalse(table.isChangeDetectionEnabled());
        }
    }

    public static class ExplodingSchemaRegistryRepository implements SchemaRegistryRepository {

        //need a real default constructor for reflection
        public ExplodingSchemaRegistryRepository() {

        }

        @Override public void init(Configuration conf) throws IOException {

        }

        @Override public String exportSchema(SchemaWriter writer, PTable table) throws IOException {
            throw new IOException("I always explode!");
        }

        @Override public String getSchemaById(String schemaId) {
            return null;
        }

        @Override public String getSchemaByTable(PTable table) {
            return null;
        }

        @Override public void close() throws IOException {

        }
    }
}
