/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.KeyValueUtil;

public class ScanProjector {
    
    public enum ProjectionType {TABLE, CF, CQ};
    
    private static final String SCAN_PROJECTOR = "scanProjector";
    private static final byte[] SEPERATOR = Bytes.toBytes(":");
    
    private final ProjectionType type;
    private final byte[] tablePrefix;
    private final Map<ImmutableBytesPtr, byte[]> cfProjectionMap;
    private final Map<ImmutableBytesPtr, Map<ImmutableBytesPtr, Pair<byte[], byte[]>>> cqProjectionMap;
    
    private ScanProjector(ProjectionType type, byte[] tablePrefix, 
            Map<ImmutableBytesPtr, byte[]> cfProjectionMap, Map<ImmutableBytesPtr, 
            Map<ImmutableBytesPtr, Pair<byte[], byte[]>>> cqProjectionMap) {
        this.type = ProjectionType.TABLE;
        this.tablePrefix = tablePrefix;
        this.cfProjectionMap = cfProjectionMap;
        this.cqProjectionMap = cqProjectionMap;
    }
    
    public static void serializeProjectorIntoScan(Scan scan, ScanProjector projector) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeVInt(output, projector.type.ordinal());
            switch (projector.type) {
            case TABLE:
                WritableUtils.writeCompressedByteArray(output, projector.tablePrefix);
                break;
            case CF:
                WritableUtils.writeVInt(output, projector.cfProjectionMap.size());
                for (Map.Entry<ImmutableBytesPtr, byte[]> entry : projector.cfProjectionMap.entrySet()) {
                    WritableUtils.writeCompressedByteArray(output, entry.getKey().get());
                    WritableUtils.writeCompressedByteArray(output, entry.getValue());
                }
                break;
            case CQ:
                WritableUtils.writeVInt(output, projector.cqProjectionMap.size());
                for (Map.Entry<ImmutableBytesPtr, Map<ImmutableBytesPtr, Pair<byte[], byte[]>>> entry : 
                    projector.cqProjectionMap.entrySet()) {
                    WritableUtils.writeCompressedByteArray(output, entry.getKey().get());
                    Map<ImmutableBytesPtr, Pair<byte[], byte[]>> map = entry.getValue();
                    WritableUtils.writeVInt(output, map.size());
                    for (Map.Entry<ImmutableBytesPtr, Pair<byte[], byte[]>> e : map.entrySet()) {
                        WritableUtils.writeCompressedByteArray(output, e.getKey().get());
                        WritableUtils.writeCompressedByteArray(output, e.getValue().getFirst());
                        WritableUtils.writeCompressedByteArray(output, e.getValue().getSecond());
                    }
                }
                break;
            default:
                throw new IOException("Unrecognized projection type '" + projector.type + "'");    
            }
            scan.setAttribute(SCAN_PROJECTOR, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public static ScanProjector deserializeProjectorFromScan(Scan scan) {
        byte[] proj = scan.getAttribute(SCAN_PROJECTOR);
        if (proj == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(proj);
        try {
            DataInputStream input = new DataInputStream(stream);
            int t = WritableUtils.readVInt(input);
            ProjectionType type = ProjectionType.values()[t];
            if (type == ProjectionType.TABLE) {
                byte[] tablePrefix = WritableUtils.readCompressedByteArray(input);
                return new ScanProjector(type, tablePrefix, null, null);
            }
            if (type == ProjectionType.CF) {
                int count = WritableUtils.readVInt(input);
                Map<ImmutableBytesPtr, byte[]> cfMap = new HashMap<ImmutableBytesPtr, byte[]>();
                for (int i = 0; i < count; i++) {
                    byte[] cf = WritableUtils.readCompressedByteArray(input);
                    byte[] renamed = WritableUtils.readCompressedByteArray(input);
                    cfMap.put(new ImmutableBytesPtr(cf), renamed);
                }
                return new ScanProjector(type, null, cfMap, null);
            }
            
            int count = WritableUtils.readVInt(input);
            Map<ImmutableBytesPtr, Map<ImmutableBytesPtr, Pair<byte[], byte[]>>> cqMap = 
                new HashMap<ImmutableBytesPtr, Map<ImmutableBytesPtr, Pair<byte[], byte[]>>>();
            for (int i = 0; i < count; i++) {
                byte[] cf = WritableUtils.readCompressedByteArray(input);
                int nQuals = WritableUtils.readVInt(input);
                Map<ImmutableBytesPtr, Pair<byte[], byte[]>> map = 
                    new HashMap<ImmutableBytesPtr, Pair<byte[], byte[]>>();
                for (int j = 0; j < nQuals; j++) {
                    byte[] cq = WritableUtils.readCompressedByteArray(input);
                    byte[] renamedCf = WritableUtils.readCompressedByteArray(input);
                    byte[] renamedCq = WritableUtils.readCompressedByteArray(input);
                    map.put(new ImmutableBytesPtr(cq), new Pair<byte[], byte[]>(renamedCf, renamedCq));
                }
                cqMap.put(new ImmutableBytesPtr(cf), map);
            }
            return new ScanProjector(type, null, null, cqMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public ProjectionType getType() {
        return this.type;
    }
    
    public byte[] getTablePrefix() {
        return this.tablePrefix;
    }
    
    public Map<ImmutableBytesPtr, byte[]> getCfProjectionMap() {
        return this.cfProjectionMap;
    }
    
    public Map<ImmutableBytesPtr, Map<ImmutableBytesPtr, Pair<byte[], byte[]>>> getCqProjectionMap() {
        return this.cqProjectionMap;
    }
    
    public KeyValue getProjectedKeyValue(KeyValue kv) {
        if (type == ProjectionType.TABLE) {
            byte[] cf = ByteUtil.concat(tablePrefix, SEPERATOR, kv.getFamily());
            return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(), 
                    cf, kv.getQualifier(), kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        }
        
        if (type == ProjectionType.CF) {
            byte[] cf = cfProjectionMap.get(new ImmutableBytesPtr(kv.getFamily()));
            if (cf == null)
                return kv;
            return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(), 
                    cf, kv.getQualifier(), kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        }
        
        Map<ImmutableBytesPtr, Pair<byte[], byte[]>> map = cqProjectionMap.get(new ImmutableBytesPtr(kv.getFamily()));
        if (map == null)
            return kv;
        
        Pair<byte[], byte[]> col = map.get(new ImmutableBytesPtr(kv.getQualifier()));
        if (col == null)
            return kv;
        
        return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(), 
                col.getFirst(), col.getSecond(), kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }
}
