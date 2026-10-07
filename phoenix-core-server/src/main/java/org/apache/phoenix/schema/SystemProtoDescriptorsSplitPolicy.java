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
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;

/**
 * Split policy for {@code SYSTEM.PROTO_DESCRIPTORS}. Mirrors {@link SystemTaskSplitPolicy} — a thin
 * no-split extension of {@link DisabledRegionSplitPolicy} so the table descriptor names a
 * Phoenix-owned class (consistent with the other {@code System*SplitPolicy} entries).
 * <p>
 * The descriptor table holds one row per registered protobuf message name; in practice this is a
 * small bounded registry (hundreds of rows, capped at 10 MiB per cell). Single-region is required
 * for {@code CREATE PROTOBUF BUNDLE} atomicity: the server-side handler atomically batch-writes N
 * descriptor rows via {@code Region.mutateRowsWithLocks}, which HBase only honors when all N keys
 * live in the same region. Per-row CREATE / ALTER / DROP work for any number of regions via
 * single-key dispatch, but the bundle case forces the structural pin.
 */
public class SystemProtoDescriptorsSplitPolicy extends DisabledRegionSplitPolicy {
  // empty
}
