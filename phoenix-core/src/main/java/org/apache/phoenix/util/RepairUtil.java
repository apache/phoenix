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
package org.apache.phoenix.util;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

public class RepairUtil {
    public static boolean isLocalIndexStoreFilesConsistent(RegionCoprocessorEnvironment environment, Store store) {
        byte[] startKey = environment.getRegion().getRegionInfo().getStartKey();
        byte[] endKey = environment.getRegion().getRegionInfo().getEndKey();
        byte[] indexKeyEmbedded = startKey.length == 0 ? new byte[endKey.length] : startKey;
        for (StoreFile file : store.getStorefiles()) {
            byte[] fileFirstRowKey = KeyValue.createKeyValueFromKey(file.getReader().getFirstKey()).getRow();;
            if ((fileFirstRowKey != null && Bytes.compareTo(file.getReader().getFirstKey(), 0, indexKeyEmbedded.length,
                    indexKeyEmbedded, 0, indexKeyEmbedded.length) != 0)
                    /*|| (endKey.length > 0 && Bytes.compareTo(file.getLastKey(), endKey) < 0)*/) { return false; }
        }
        return true;
    }

}
