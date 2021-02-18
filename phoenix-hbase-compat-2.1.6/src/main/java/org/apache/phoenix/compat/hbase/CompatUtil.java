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
package org.apache.phoenix.compat.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

import java.io.IOException;

public class CompatUtil {

    private CompatUtil() {
        // Not to be instantiated
    }

    public static int getCellSerializedSize(Cell cell) {
        return org.apache.hadoop.hbase.KeyValueUtil.length(cell);
    }

    public static ListMultimap<String, ? extends Permission> readPermissions(byte[] data,
            Configuration conf) throws DeserializationException {
        return AccessControlLists.readPermissions(data, conf);
    }

    public static HFileContextBuilder withComparator(HFileContextBuilder contextBuilder,
            CellComparatorImpl cellComparator) {
        return contextBuilder;
    }

    public static StoreFileWriter.Builder withComparator(StoreFileWriter.Builder builder,
            CellComparatorImpl cellComparator) {
        return builder.withComparator(cellComparator);
    }

    public static Scan getScanForTableName(Connection conn, TableName tableName) {
        return MetaTableAccessor.getScanForTableName(conn, tableName);
    }

    /**
     * HBase 2.3+ has storeRefCount available in RegionMetrics
     *
     * @param admin Admin instance
     * @return true if any region has refCount leakage
     * @throws IOException if something went wrong while connecting to Admin
     */
    public static boolean isAnyStoreRefCountLeaked(Admin admin)
            throws IOException {
        return false;
    }

    public static ChecksumType getChecksumType(Configuration conf) {
        return HStore.getChecksumType(conf);
    }

    public static int getBytesPerChecksum(Configuration conf) {
        return HStore.getBytesPerChecksum(conf);
    }

}
