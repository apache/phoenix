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
import org.apache.phoenix.query.QueryConstants;
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
     * @param doRetry
     * @throws SQLException
     */
    public static void validateLastDDLTimestamp(PhoenixConnection conn,
                                                List<TableRef> allTableRefs,
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
                    = getValidateDDLTimestampRequest(conn, tableRefs);
            service.validateLastDDLTimestamp(null, request);
        } catch (Exception e) {
            // throw the Exception if a table was not found when forming the request
            // so that we can update cache and retry
            if (e instanceof TableNotFoundException) {
                throw (TableNotFoundException)e;
            }
            SQLException parsedException = ClientUtil.parseServerException(e);
            if (parsedException instanceof StaleMetadataCacheException) {
                throw parsedException;
            }
            //retry once for any exceptions other than StaleMetadataCacheException
            LOGGER.error("Error in validating DDL timestamp for {}", infoString, parsedException);
            if (doRetry) {
                // update the list of live region servers
                conn.getQueryServices().refreshLiveRegionServers();
                validateLastDDLTimestamp(conn, tableRefs, false);
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
     * 3. Add all indexes of a table/view in case index state was changed.
     * @param conn
     * @param tableRefs
     * @return ValidateLastDDLTimestampRequest for the table in tableRef
     */
    private static RegionServerEndpointProtos.ValidateLastDDLTimestampRequest
        getValidateDDLTimestampRequest(PhoenixConnection conn, List<TableRef> tableRefs)
            throws TableNotFoundException {

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
                setLastDDLTimestampRequestParameters(conn, innerBuilder, parentTable);
                requestBuilder.addLastDDLTimestampRequests(innerBuilder);
            }

            // add the tableRef to the request
            innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
            setLastDDLTimestampRequestParameters(conn, innerBuilder, tableRef.getTable());
            requestBuilder.addLastDDLTimestampRequests(innerBuilder);

            //when querying a view, we need to validate last ddl timestamps for all its ancestors
            if (PTableType.VIEW.equals(tableRef.getTable().getType())) {
                PTable pTable = tableRef.getTable();
                // view name and parent name can be same for a mapped view
                while (pTable.getParentName() != null &&
                            !pTable.getName().equals(pTable.getParentName())) {
                    PTable parentTable = getPTableFromCache(conn, conn.getTenantId(),
                                                            pTable.getParentName().getString());
                    innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                    setLastDDLTimestampRequestParameters(conn, innerBuilder, parentTable);
                    requestBuilder.addLastDDLTimestampRequests(innerBuilder);
                    pTable = parentTable;
                }
            }

            //validate all indexes of a table/view for any changes
            //in case index state was changed.
            for (PTable idxPTable : tableRef.getTable().getIndexes()) {
                innerBuilder = RegionServerEndpointProtos.LastDDLTimestampRequest.newBuilder();
                setLastDDLTimestampRequestParameters(conn, innerBuilder, idxPTable);
                requestBuilder.addLastDDLTimestampRequests(innerBuilder);
            }
        }

        return requestBuilder.build();
    }

    /**
     * For the given PTable, set the attributes on the LastDDLTimestampRequest.
     */
    private static void setLastDDLTimestampRequestParameters (
            PhoenixConnection conn,
            RegionServerEndpointProtos.LastDDLTimestampRequest.Builder builder,
            PTable pTable) throws TableNotFoundException {
        PName tenantID = pTable.getTenantId();
        PName tableName = pTable.getTableName();
        PName schemaName = pTable.getSchemaName();

        //Inherited view indexes do not exist is SYSTEM.CATALOG, add the parent index.
        if (pTable.getType().equals(PTableType.INDEX) &&
                pTable.getName().getString().contains(
                        QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR)) {
            String[] parentNames = pTable.getName().getString()
                    .split(QueryConstants.CHILD_VIEW_INDEX_NAME_SEPARATOR);
            String parentIndexName = parentNames[parentNames.length-1];
            PTable parentIndexTable
                    = getPTableFromCache(conn, conn.getTenantId(), parentIndexName);
            tableName = parentIndexTable.getTableName();
            tenantID= parentIndexTable.getTenantId();
            schemaName = parentIndexTable.getSchemaName();
        }

        byte[] tenantIDBytes = (tenantID == null)
                ? HConstants.EMPTY_BYTE_ARRAY
                : tenantID.getBytes();
        byte[] schemaNameBytes = (schemaName == null)
                ? HConstants.EMPTY_BYTE_ARRAY
                : schemaName.getBytes();

        builder.setTenantId(ByteStringer.wrap(tenantIDBytes));
        builder.setSchemaName(ByteStringer.wrap(schemaNameBytes));
        builder.setTableName(ByteStringer.wrap(tableName.getBytes()));
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
