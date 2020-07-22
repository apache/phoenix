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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;


public class CompatUtil {

    private CompatUtil() {
        //Not to be instantiated
    }

    public static int getCellSerializedSize(Cell cell) {
        return cell.getSerializedSize();
    }

    public static ListMultimap<String, ? extends Permission> readPermissions(
            byte[] data, Configuration conf) throws DeserializationException {
        return PermissionStorage.readPermissions(data, conf);
    }

    public static HFileContext createHFileContext(Configuration conf, Algorithm compression,
            Integer blockSize, DataBlockEncoding encoding, CellComparator comparator) {

        return new HFileContextBuilder()
            .withCompression(compression)
            .withChecksumType(HStore.getChecksumType(conf))
            .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
            .withBlockSize(blockSize)
            .withDataBlockEncoding(encoding)
            .build();
    }

    public static HFileContextBuilder withComparator(HFileContextBuilder contextBuilder,
            CellComparatorImpl cellComparator) {
        return contextBuilder.withCellComparator(cellComparator);
    }

    public static StoreFileWriter.Builder withComparator(StoreFileWriter.Builder builder,
            CellComparatorImpl cellComparator) {
        return builder;
    }
}
