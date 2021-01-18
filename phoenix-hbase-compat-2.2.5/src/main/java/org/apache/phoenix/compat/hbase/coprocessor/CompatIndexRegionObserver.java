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
package org.apache.phoenix.compat.hbase.coprocessor;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;

import java.util.HashMap;
import java.util.Map;

public class CompatIndexRegionObserver implements RegionObserver {

    public void preWALAppend(ObserverContext<RegionCoprocessorEnvironment> c, WALKey key,
                             WALEdit edit) {
        //no-op implementation for HBase 2.1 and 2.2 that doesn't support this co-proc hook.
    }

    public static void appendToWALKey(WALKey key, String attrKey, byte[] attrValue) {
        //no-op for HBase 2.1 and 2.2 because we don't have WALKey.addExtendedAttribute(String,
        // byte[])
    }

    public static byte[] getAttributeValueFromWALKey(WALKey key, String attrKey) {
        return null;
    }

    public static Map<String, byte[]> getAttributeValuesFromWALKey(WALKey key) {
        return new HashMap<String, byte[]>();
    }
}
