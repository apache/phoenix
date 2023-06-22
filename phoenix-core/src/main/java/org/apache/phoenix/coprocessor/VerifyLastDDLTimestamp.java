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

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.coprocessor.generated.DDLTimestampMaintainersProtos;
import org.apache.phoenix.util.LastDDLTimestampMaintainerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LAST_DDL_TIMESTAMP_MAINTAINERS;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LAST_DDL_TIMESTAMP_MAINTAINERS_VERIFIED;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;

/**
 * Client provides last DDL timestamp of tables/views/indexes included in read/write operation
 * through scan or mutation {@link BaseScannerRegionObserver#LAST_DDL_TIMESTAMP_MAINTAINERS}
 * attribute.
 * This verifies that client has the latest version of LAST_DDL_TIMESTAMP version.
 * If client's provided LAST_DDL_TIMESTAMP is less than what is present in SYSTEM.CATALOG
 * then it throws StaleMetadataCacheException. Once it verifies the request,
 * it will set LAST_DDL_TIMESTAMP_MAINTAINERS_VERIFIED attribute to true so that
 * other co-processors within the same session don't validate the LAST_DDL_TIMESTAMP again.
 */
public class VerifyLastDDLTimestamp {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyLastDDLTimestamp.class);

    public static void verifyLastDDLTimestamp(MiniBatchOperationInProgress<Mutation> miniBatchOp,
                                              RegionCoprocessorEnvironment env) throws IOException {

        byte[] maintainersBytes = miniBatchOp.getOperation(0).getAttribute(
                LAST_DDL_TIMESTAMP_MAINTAINERS);
        if (maintainersBytes == null) {
            // Client doesn't support sending LAST_DDL_TIMESTAMP_MAINTAINERS. Do nothing.
            return;
        }
        DDLTimestampMaintainersProtos.DDLTimestampMaintainers maintainers =
                LastDDLTimestampMaintainerUtil.deserialize(maintainersBytes);
        verifyLastDDLTimestampInternal(maintainers, env);
    }

    /**
     * Verify that LAST_DDL_TIMESTAMP provided by the client is upto date. If it is stale it will
     * throw StaleMetadataCacheException.
     * This method will be called by each coprocessor in the co-proc chain. The first co-proc in
     * the chain will verify LAST_DDL_TIMESTAMP and set LAST_DDL_TIMESTAMP_MAINTAINERS_VERIFIED
     * attribute to true in the scan. All the co-proc will check for the
     * LAST_DDL_TIMESTAMP_MAINTAINERS_VERIFIED attribute's value and verify only if
     * this attribute is set to false.
     * @param scan scan provided by the client.
     * @param env region coprocessor environment.
     * @throws IOException
     */
    public static void verifyLastDDLTimestamp(Scan scan, RegionCoprocessorEnvironment env)
            throws IOException {
        // Check whether any previous co-processor have already verified
        // LAST_DDL_TIMESTAMP_MAINTAINERS
        if (Bytes.equals(scan.getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS_VERIFIED), TRUE_BYTES)) {
            // Already verified LAST_DDL_TIMESTAMP_MAINTAINERS
            LOGGER.trace("Already verified LAST_DDL_TIMESTAMP_MAINTAINERS for this request");
            return;
        }
        byte[] maintainersBytes = scan.getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS);
        if (maintainersBytes == null) {
            // TODO Either the feature is disabled or client is old. Return for now
            //  but need to think how to handle this situation.
            return;
        }
        DDLTimestampMaintainersProtos.DDLTimestampMaintainers maintainers
                = LastDDLTimestampMaintainerUtil.deserialize(maintainersBytes);
        verifyLastDDLTimestampInternal(maintainers, env);
        scan.setAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS_VERIFIED, TRUE_BYTES);
    }

    /**
     * Verify LAST_DDL_TIMESTAMP provided by client are upto date.
     * @param maintainers DDLTimestampMaintainers provided by client
     * @param env region coprocessor environment.
     * @throws IOException
     */
    private static void verifyLastDDLTimestampInternal(
            DDLTimestampMaintainersProtos.DDLTimestampMaintainers maintainers,
            RegionCoprocessorEnvironment env) throws IOException {
        ServerMetadataCache cache = ServerMetadataCache.getInstance(env);
        List<DDLTimestampMaintainersProtos.DDLTimestampMaintainer> maintainerList =
                maintainers.getDDLTimestampMaintainersList();
        // For tables with no index, it will have just 1 DDLTimestampMaintainer for the table itself
        // For tables with index, it will 1 DDLTimestampMaintainer for base table
        // and 1 DDLTimestampMaintainer for index table.
        // For table with views, it will 1 DDLTimestampMaintainer for the base table,
        // 1 DDLTimestampMaintainer for the queried view and 1 DDLTimestampMaintainer
        // each for the parent views.
        for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer: maintainerList) {
            long lastDDLTimestamp = cache.getLastDDLTimestampForTable(maintainer);
            long clientLastDDLTimestamp = maintainer.getLastDDLTimestamp();
            if (clientLastDDLTimestamp < lastDDLTimestamp) {
                // TODO Log and throw StaleMetadataCacheException in future.
                LOGGER.info("Stale metadata for LAST_DDL_TIMESTAMP for tenantID: {}, schema: {}," +
                        " table: {}, client provided timestamp: {}, server timestamp: {}",
                        maintainer.getTenantID().toStringUtf8(),
                        maintainer.getSchemaName().toStringUtf8(),
                        maintainer.getTableName().toStringUtf8(),
                        maintainer.getLastDDLTimestamp(), lastDDLTimestamp);
            }
        }
    }
}