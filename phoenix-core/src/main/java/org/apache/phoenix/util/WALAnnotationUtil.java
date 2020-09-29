/**
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

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.phoenix.compat.hbase.coprocessor.CompatIndexRegionObserver;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionObserver;

import java.util.Map;

/**
 * Utility functions shared between IndexRegionObserver and MutationState for annotating the
 * HBase WAL with Phoenix-level metadata about mutations.
 */
public class WALAnnotationUtil {

    public static void appendMutationAttributesToWALKey(WALKey key,
                                        IndexRegionObserver.BatchMutateContext context) {
        if (context != null && context.getOriginalMutations().size() > 0) {
            Mutation firstMutation = context.getOriginalMutations().get(0);
            Map<String, byte[]> attrMap = firstMutation.getAttributesMap();
            for (MutationState.MutationMetadataType metadataType :
                MutationState.MutationMetadataType.values()) {
                String metadataTypeKey = metadataType.toString();
                if (attrMap.containsKey(metadataTypeKey)) {
                    CompatIndexRegionObserver.appendToWALKey(key, metadataTypeKey,
                        attrMap.get(metadataTypeKey));
                }
            }
        }
    }

    /**
     * Add metadata about a mutation onto the attributes of the mutation. This will be written as
     * an annotation into the HBase write ahead log (WAL) when the mutation is processed
     * server-side, usually in IndexRegionObserver
     * @param m Mutation
     * @param tenantId Tenant (if for a tenant-owned view) otherwise null
     * @param schemaName Schema name
     * @param logicalTableName Logical name of the table or view
     * @param tableType Table type (e.g table, view)
     * @param ddlTimestamp Server-side timestamp when the table/view was created or last had a
     *                     column added or dropped from it, whichever is greater
     */
    public static void annotateMutation(Mutation m, byte[] tenantId, byte[] schemaName,
                                        byte[] logicalTableName, byte[] tableType, byte[] ddlTimestamp) {
        if (!m.getDurability().equals(Durability.SKIP_WAL)) {
            if (tenantId != null) {
                m.setAttribute(MutationState.MutationMetadataType.TENANT_ID.toString(), tenantId);
            }
            m.setAttribute(MutationState.MutationMetadataType.SCHEMA_NAME.toString(), schemaName);
            m.setAttribute(MutationState.MutationMetadataType.LOGICAL_TABLE_NAME.toString(),
                logicalTableName);
            m.setAttribute(MutationState.MutationMetadataType.TABLE_TYPE.toString(), tableType);
            m.setAttribute(MutationState.MutationMetadataType.TIMESTAMP.toString(), ddlTimestamp);
        }
    }
}
