/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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
package org.apache.phoenix.util;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.exception.StaleMetadataCacheException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for last ddl timestamp validation from the client.
 */
public class ValidateLastDDLTimestampUtil {

    private ValidateLastDDLTimestampUtil() {}

    private static final Logger LOGGER = LoggerFactory
            .getLogger(ValidateLastDDLTimestampUtil.class);

    public static String getInfoString(PName tenantId, List<TableRef> tableRefs) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Tenant: %s, ", tenantId));
        for (TableRef tableRef : tableRefs) {
            sb.append(String.format("{Schema: %s, Table: %s},",
                    tableRef.getTable().getSchemaName(),
                    tableRef.getTable().getTableName()));
        }
        return sb.toString();
    }

    /**
     * Get whether last ddl timestamp validation is enabled on the connection
     * @param connection
     * @return true if it is enabled, false otherwise
     */
    public static boolean getValidateLastDdlTimestampEnabled(PhoenixConnection connection) {
        return connection.getQueryServices().getProps()
                .getBoolean(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED,
                        QueryServicesOptions.DEFAULT_LAST_DDL_TIMESTAMP_VALIDATION_ENABLED);
    }

    /**
     * Verifies that table metadata for given tables is up-to-date in client cache with server.
     * A random live region server is picked for invoking the RPC to validate LastDDLTimestamp.
     * Retry once if there was an error performing the RPC, otherwise throw the Exception.
     * @param tableRefs
     * @param isWritePath
     * @param doRetry
     * @throws SQLException
     */
    public static void validateLastDDLTimestamp(
            PhoenixConnection conn, List<TableRef> tableRefs, boolean isWritePath, boolean doRetry)
            throws SQLException {

        String infoString = getInfoString(conn.getTenantId(), tableRefs);
        try (Admin admin = conn.getQueryServices().getAdmin()) {
            // get all live region servers
            List<ServerName> regionServers
                    = conn.getQueryServices().getLiveRegionServers();
            // pick one at random
            ServerName regionServer
                    = regionServers.get(ThreadLocalRandom.current().nextInt(regionServers.size()));

            LOGGER.debug("Sending DDL timestamp validation request for {} to regionserver {}",
                    infoString, regionServer);

            // RPC
            RegionServerEndpointProtos.RegionServerEndpointService.BlockingInterface
                    service = RegionServerEndpointProtos.RegionServerEndpointService
                    .newBlockingStub(admin.coprocessorService(regionServer));
            RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request
                    = getValidateDDLTimestampRequest(conn, tableRefs, isWritePath);
            service.validateLastDDLTimestamp(null, request);
        } catch (Exception e) {
            SQLException parsedException = ClientUtil.parseServerException(e);
            if (parsedException instanceof StaleMetadataCacheException) {
                throw parsedException;
            }
            //retry once for any exceptions other than StaleMetadataCacheException
            LOGGER.error("Error in validating DDL timestamp for {}", infoString, parsedException);
            if (doRetry) {
                // update the list of live region servers
                conn.getQueryServices().refreshLiveRegionServers();
                validateLastDDLTimestamp(conn, tableRefs, isWritePath, false);
                return;
            }
            throw parsedException;
        }
    }

    /**
     * Build a request for the validateLastDDLTimestamp RPC for the given tables.
     * 1. For a view, we need to add all its ancestors to the request
     *    in case something changed in the hierarchy.
     * 2. For an index, we need to add its parent table to the request
     *    in case the index was dropped.
     * 3. On the write path, we need to add all indexes of a table/view
     *    in case index state was changed.
     * @param conn
     * @param tableRefs
     * @param isWritePath
     * @return ValidateLastDDLTimestampRequest for the table in tableRef
     */
    private static RegionServerEndpointProtos.ValidateLastDDLTimestampRequest
        getValidateDDLTimestampRequest(PhoenixConnection conn, List<TableRef> tableRefs,
                                        boolean isWritePath) {

        RegionServerEndpointProtos.ValidateLastDDLTimestampRequest.Builder requestBuilder
                = RegionServerEndpointProtos.ValidateLastDDLTimestampRequest.newBuilder();
        RegionServerEndpointProtos.LastDDLTimestampRequest.Builder innerBuilder;

        for (TableRef tableRef : tableRefs) {

            // validate all ancestors of this PTable if any
            // index -> base table
            // view -> parent view and its ancestors
            // view index -> view and its ancestors
            for (Map.Entry<PTableKey, Long> entry
                    : tableRef.getTable().getAncestorLastDDLTimestampMap().entrySet()) {
                innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                PTableKey ancestorKey = entry.getKey();
                setLastDDLTimestampRequestParameters(innerBuilder, ancestorKey, entry.getValue());
                requestBuilder.addLastDDLTimestampRequests(innerBuilder);
            }

            // add the current table to the request
            PTable ptable = tableRef.getTable();
            innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
            setLastDDLTimestampRequestParameters(innerBuilder, ptable.getKey(),
                                                    ptable.getLastDDLTimestamp());
            requestBuilder.addLastDDLTimestampRequests(innerBuilder);

            //on the write path, we need to validate all indexes of a table/view
            //in case index state was changed
            if (isWritePath) {
                for (PTable idxPTable : tableRef.getTable().getIndexes()) {
                    innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                    setLastDDLTimestampRequestParameters(innerBuilder, idxPTable.getKey(),
                                                            idxPTable.getLastDDLTimestamp());
                    requestBuilder.addLastDDLTimestampRequests(innerBuilder);
                }
            }
        }
        return requestBuilder.build();
    }

    /**
     * For the given PTable, set the attributes on the LastDDLTimestampRequest.
     */
    private static void setLastDDLTimestampRequestParameters(
            RegionServerEndpointProtos.LastDDLTimestampRequest.Builder builder,
            PTableKey key, long lastDDLTimestamp) {
        byte[] tenantIDBytes = key.getTenantId() == null
                ? HConstants.EMPTY_BYTE_ARRAY
                : key.getTenantId().getBytes();
        byte[] schemaBytes = key.getSchemaName() == null
                ?   HConstants.EMPTY_BYTE_ARRAY
                : key.getSchemaName().getBytes();
        builder.setTenantId(ByteStringer.wrap(tenantIDBytes));
        builder.setSchemaName(ByteStringer.wrap(schemaBytes));
        builder.setTableName(ByteStringer.wrap(key.getTableName().getBytes()));
        builder.setLastDDLTimestamp(lastDDLTimestamp);
    }
}
