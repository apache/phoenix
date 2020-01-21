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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;

public class CompatUtil {

    private CompatUtil() {
        //Not to be instantiated
    }

    public static List<RegionInfo> getMergeRegions(Connection conn, byte[] regionName) 
            throws IOException {
        Pair<RegionInfo, RegionInfo> regionPair = 
                MetaTableAccessor.getRegionsFromMergeQualifier(conn,regionName);
        List<RegionInfo> regionList = new ArrayList<RegionInfo>(2);
        regionList.add(regionPair.getFirst());
        regionList.add(regionPair.getSecond());
        return regionList;
    }

    public static int getCellSerializedSize(Cell cell) {
        return org.apache.hadoop.hbase.KeyValueUtil.length(cell);
    }
}
