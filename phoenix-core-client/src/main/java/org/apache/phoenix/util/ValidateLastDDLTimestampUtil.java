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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

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
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
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
    private static final List<PTableType> ALLOWED_PTABLE_TYPES =
            Arrays.asList(PTableType.TABLE, PTableType.VIEW, PTableType.INDEX, PTableType.SYSTEM);

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
     * @param allTableRefs
     * @param isWritePath
     * @param doRetry
     * @throws SQLException
     */
    public static void validateLastDDLTimestamp(PhoenixConnection conn,
                                                List<TableRef> allTableRefs,
                                                boolean isWritePath,
                                                boolean doRetry) throws SQLException {
        List<TableRef> tableRefs = filterTableRefs(allTableRefs);
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
                                        boolean isWritePath) throws TableNotFoundException {

        RegionServerEndpointProtos.ValidateLastDDLTimestampRequest.Builder requestBuilder
                = RegionServerEndpointProtos.ValidateLastDDLTimestampRequest.newBuilder();
        RegionServerEndpointProtos.LastDDLTimestampRequest.Builder innerBuilder;

        for (TableRef tableRef : tableRefs) {

            //when querying an index, we need to validate its parent table
            //in case the index was dropped
            if (PTableType.INDEX.equals(tableRef.getTable().getType())) {
                innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                PTable parentTable
                        = getPTableFromCache(conn, conn.getTenantId(),
                                             tableRef.getTable().getParentName().getString());
                setLastDDLTimestampRequestParameters(innerBuilder, conn.getTenantId(), parentTable);
                requestBuilder.addLastDDLTimestampRequests(innerBuilder);
            }

            // add the tableRef to the request
            innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
            setLastDDLTimestampRequestParameters(
                    innerBuilder, conn.getTenantId(), tableRef.getTable());
            requestBuilder.addLastDDLTimestampRequests(innerBuilder);

            //when querying a view, we need to validate last ddl timestamps for all its ancestors
            if (PTableType.VIEW.equals(tableRef.getTable().getType())) {
                PTable pTable = tableRef.getTable();
                while (pTable.getParentName() != null) {
                    PTable parentTable = getPTableFromCache(conn, conn.getTenantId(),
                                                            pTable.getParentName().getString());
                    innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                    setLastDDLTimestampRequestParameters(
                            innerBuilder, conn.getTenantId(), parentTable);
                    requestBuilder.addLastDDLTimestampRequests(innerBuilder);
                    pTable = parentTable;
                }
            }

            //on the write path, we need to validate all indexes of a table/view
            //in case index state was changed
            if (isWritePath) {
                for (PTable idxPTable : tableRef.getTable().getIndexes()) {
                    innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                    setLastDDLTimestampRequestParameters(
                            innerBuilder, conn.getTenantId(), idxPTable);
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
            PName tenantId, PTable pTable) {
        byte[] tenantIDBytes = tenantId == null
                ? HConstants.EMPTY_BYTE_ARRAY
                : tenantId.getBytes();
        byte[] schemaBytes = pTable.getSchemaName() == null
                ?   HConstants.EMPTY_BYTE_ARRAY
                : pTable.getSchemaName().getBytes();
        builder.setTenantId(ByteStringer.wrap(tenantIDBytes));
        builder.setSchemaName(ByteStringer.wrap(schemaBytes));
        builder.setTableName(ByteStringer.wrap(pTable.getTableName().getBytes()));
        builder.setLastDDLTimestamp(pTable.getLastDDLTimestamp());
    }

    /**
     * Filter out any TableRefs which are not tables, views or indexes.
     * @param tableRefs
     * @return
     */
    private static List<TableRef> filterTableRefs(List<TableRef> tableRefs) {
        List<TableRef> filteredTableRefs = tableRefs.stream()
                .filter(tableRef -> ALLOWED_PTABLE_TYPES.contains(tableRef.getTable().getType()))
                .collect(Collectors.toList());
        return filteredTableRefs;
    }

    /**
     * Return the PTable object from this client's cache.
     * For tenant specific connection, also look up the global table without using tenantId.
     * @param conn
     * @param tenantId
     * @param tableName
     * @return PTable object from the cache for the given tableName
     * @throws TableNotFoundException
     */
    private static PTable getPTableFromCache(PhoenixConnection conn,
                                             PName tenantId,
                                             String tableName) throws TableNotFoundException {
        PTableKey key = new PTableKey(tenantId, tableName);
        PTable table;
        try {
            table = conn.getTable(key);
        }
        catch (TableNotFoundException e) {
            if (tenantId != null) {
                table = getPTableFromCache(conn, null, tableName);
            } else {
                throw e;
            }
        }
        return table;
    }
}
