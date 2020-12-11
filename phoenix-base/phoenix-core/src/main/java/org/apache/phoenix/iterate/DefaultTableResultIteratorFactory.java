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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.cache.ServerCacheClient.ServerCache;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.schema.TableRef;

public class DefaultTableResultIteratorFactory implements TableResultIteratorFactory {

       @Override
    public TableResultIterator newIterator(MutationState mutationState, TableRef tableRef,
            Scan scan, ScanMetricsHolder scanMetricsHolder, long renewLeaseThreshold,
            QueryPlan plan, ParallelScanGrouper scanGrouper, Map<ImmutableBytesPtr,ServerCache> caches) throws SQLException {
        return new TableResultIterator(mutationState, scan, scanMetricsHolder, renewLeaseThreshold,
                plan, scanGrouper, caches);
    }

}
