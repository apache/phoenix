/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.phoenix.coprocessor.generated.DDLTimestampMaintainersProtos;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LAST_DDL_TIMESTAMP_MAINTAINERS;

/**
 * Util for verifying LastDDLTimestamps.
 */
public class LastDDLTimestampMaintainerUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(LastDDLTimestampMaintainerUtil.class);

    /**
     * Sets the LAST_DDL_TIMESTAMP maintainers to each mutation attribute.
     * @param mutationList list of mutations
     * @param connection phoenix connection
     * @param table table
     * @throws SQLException
     */
    public static void getLastDDLTimestampMaintainers(List<Mutation> mutationList, PhoenixConnection connection, PTable table)
            throws SQLException {
        byte[] maintainers = getLastDDLTimestampMaintainers(table, connection);
        if (maintainers == null) {
            // This means it is a system table.
            return;
        }
        for (Mutation mutation: mutationList) {
            mutation.setAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS, maintainers);
        }
    }

    /**
     * Sets the LAST_DDL_TIMESTAMP maintainers to the scan attribute
     * @param scan
     * @param table
     * @param connection
     * @throws SQLException
     */
    public static void getLastDDLTimestampMaintainers(Scan scan, PTable table, PhoenixConnection connection)
            throws SQLException {
        byte[] maintainers = getLastDDLTimestampMaintainers(table, connection);
        if (maintainers == null) {
            // This means it is a system table.
            return;
        }
        scan.setAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS, maintainers);
    }

    /**
     * Constructs the LAST_DDL_TIMESTAMP maintainers for the given table. We skip if the table is a SYSTEM_CATALOG
     * table.
     * @param table table
     * @param connection phoenix connection
     * @return maintainers object
     * @throws SQLException
     */
    public static byte[] getLastDDLTimestampMaintainers(PTable table, PhoenixConnection connection)
        throws SQLException {
        // TODO Skip setting last ddl timestamp attribute on SYSTEM tables for now. Think how can we handle SYSTEM tables
        if (PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA.equals(table.getSchemaName().getString())) {
            LOGGER.info("RSS Skipping");
            return null;
        }
        // TODO Think of better way to convert bytes to bytesString.
        DDLTimestampMaintainersProtos.DDLTimestampMaintainers.Builder builder
                = DDLTimestampMaintainersProtos.DDLTimestampMaintainers.newBuilder();

        // Add LastDDLTimestampMaintainer for the referenced table/view/index.
        builder.addDDLTimestampMaintainers(getLastDDLTimestampMaintainerBuilder(table));

        // TODO We don't have to add indexes if it is a read request and table scanned to base table.
        // TODO Only for upsert request on a base table, we have to add indexes.
        // For view or table with indexes, add them to the builder
        for (PTable index: table.getIndexes()) {
            DDLTimestampMaintainersProtos.DDLTimestampMaintainer.Builder indexMaintainerBuilder =
                    getLastDDLTimestampMaintainerBuilder(index);
            builder.addDDLTimestampMaintainers(indexMaintainerBuilder);
        }

        // For view, resolve all the parent views all the way upto the base table.
        if (table.getType() == PTableType.VIEW) {
            PName parentTableName = table.getParentTableName();
            PName parentSchemaName = table.getParentSchemaName();

            while (parentTableName != null) {
                String parentTableNameStr = parentTableName.getString();
                String parentSchemaNameStr = parentSchemaName == null ? null : parentSchemaName.getString();
                String fullParentTableStr = SchemaUtil.getTableName(parentSchemaNameStr, parentTableNameStr);
                PTable parentTable = PhoenixRuntime.getTable(connection, fullParentTableStr);
                builder.addDDLTimestampMaintainers(getLastDDLTimestampMaintainerBuilder(parentTable));
                parentTableName = parentTable.getParentTableName();
                parentSchemaName = parentTable.getParentSchemaName();
            }
        }
        return builder.build().toByteArray();
    }

    // This table can be base table, index or view.
    private static DDLTimestampMaintainersProtos.DDLTimestampMaintainer.Builder
    getLastDDLTimestampMaintainerBuilder(PTable table) {
        DDLTimestampMaintainersProtos.DDLTimestampMaintainer.Builder maintainerBuilder =
                DDLTimestampMaintainersProtos.DDLTimestampMaintainer.newBuilder();
        if (table.getTenantId() != null) {
            maintainerBuilder.setTenantID(ByteStringer.wrap(table.getTenantId().getBytes()));
        }
        if (table.getSchemaName() != null) {
            maintainerBuilder.setSchemaName(ByteStringer.wrap(table.getSchemaName().getBytes()));
        }
        maintainerBuilder.setTableName(ByteStringer.wrap(table.getTableName().getBytes()));
        maintainerBuilder.setLastDDLTimestamp(table.getLastDDLTimestamp());
        return maintainerBuilder;
    }
}
