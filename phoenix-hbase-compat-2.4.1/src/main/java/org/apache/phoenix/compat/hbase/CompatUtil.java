/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CompatUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        CompatUtil.class);

    private static boolean hasFixedShortCircuitConnection =
            VersionInfo.compareVersion(VersionInfo.getVersion(), "2.4.12") >= 0;

    private CompatUtil() {
        //Not to be instantiated
    }

    public static HFileContext createHFileContext(Configuration conf, Algorithm compression,
            Integer blockSize, DataBlockEncoding encoding, CellComparator comparator) {

        return new HFileContextBuilder()
            .withCompression(compression)
            .withChecksumType(StoreUtils.getChecksumType(conf))
            .withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(conf))
            .withBlockSize(blockSize)
            .withDataBlockEncoding(encoding)
            .build();
    }

    public static List<RegionInfo> getMergeRegions(Connection conn, RegionInfo regionInfo)
            throws IOException {
        return MetaTableAccessor.getMergeRegions(conn, regionInfo.getRegionName());
    }

    public static ChecksumType getChecksumType(Configuration conf) {
        return StoreUtils.getChecksumType(conf);
    }

    public static int getBytesPerChecksum(Configuration conf) {
        return StoreUtils.getBytesPerChecksum(conf);
    }

    public static Connection createShortCircuitConnection(final Configuration configuration,
            final RegionCoprocessorEnvironment env) throws IOException {
        if (hasFixedShortCircuitConnection) {
            return env.createConnection(configuration);
        } else {
            return org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(configuration);
        }
    }
}
