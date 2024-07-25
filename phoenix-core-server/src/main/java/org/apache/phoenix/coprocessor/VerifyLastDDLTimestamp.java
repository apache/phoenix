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
package org.apache.phoenix.coprocessor;

import java.sql.SQLException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.exception.StaleMetadataCacheException;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client provides last DDL timestamp of tables/views/indexes included in read/write operation
 * This verifies that client has the latest version of LAST_DDL_TIMESTAMP version.
 * If client's provided LAST_DDL_TIMESTAMP is less than what is present in SYSTEM.CATALOG
 * then it throws StaleMetadataCacheException.
 */
public class VerifyLastDDLTimestamp {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyLastDDLTimestamp.class);

    private VerifyLastDDLTimestamp() {
        // Not to be instantiated.
    }

    /**
     * Verify that LAST_DDL_TIMESTAMP provided by the client is up to date. If it is stale it will
     * throw StaleMetadataCacheException.
     *
     * @param tenantID               tenant id
     * @param schemaName             schema name
     * @param tableName              table name
     * @param clientLastDDLTimestamp last ddl timestamp provided by client
     * @param cache                  ServerMetadataCache
     * @throws SQLException         StaleMetadataCacheException if client provided timestamp
     *                              is stale.
     */
    public static void verifyLastDDLTimestamp(ServerMetadataCache cache, byte[] tenantID,
                              byte[] schemaName, byte[] tableName, long clientLastDDLTimestamp)
            throws SQLException {
        long lastDDLTimestamp = cache.getLastDDLTimestampForTable(tenantID, schemaName, tableName);
        // Is it possible to have client last ddl timestamp greater than server side?
        if (clientLastDDLTimestamp < lastDDLTimestamp) {
            LOGGER.error("Stale metadata for LAST_DDL_TIMESTAMP for tenantID: {}, schema: {},"
                            + " table: {}, client provided timestamp: {}, server timestamp: {}",
                    Bytes.toString(tenantID), Bytes.toString(schemaName),
                    Bytes.toString(tableName), clientLastDDLTimestamp, lastDDLTimestamp);
            String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
            throw new StaleMetadataCacheException("Stale metadata cache for table name: "
                    + fullTableName);
        }
    }
}
