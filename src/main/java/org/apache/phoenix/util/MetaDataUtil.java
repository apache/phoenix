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
package org.apache.phoenix.util;

import static org.apache.phoenix.util.SchemaUtil.getVarChars;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;


public class MetaDataUtil {

    public static boolean areClientAndServerCompatible(long version) {
        // A server and client with the same major and minor version number must be compatible.
        // So it's important that we roll the PHOENIX_MAJOR_VERSION or PHOENIX_MINOR_VERSION
        // when we make an incompatible change.
        return areClientAndServerCompatible(MetaDataUtil.decodePhoenixVersion(version), MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION);
    }

    // For testing
    static boolean areClientAndServerCompatible(int version, int pMajor, int pMinor) {
        // A server and client with the same major and minor version number must be compatible.
        // So it's important that we roll the PHOENIX_MAJOR_VERSION or PHOENIX_MINOR_VERSION
        // when we make an incompatible change.
        return MetaDataUtil.encodeMaxPatchVersion(pMajor, pMinor) >= version && MetaDataUtil.encodeMinPatchVersion(pMajor, pMinor) <= version;
    }

    // Given the encoded integer representing the phoenix version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodePhoenixVersion(long version) {
        return (int) ((version << Byte.SIZE * 3) >>> Byte.SIZE * 4);
    }
    
    // TODO: generalize this to use two bytes to return a SQL error code instead
    public static long encodeMutableIndexConfiguredProperly(long version, boolean isValid) {
        if (!isValid) {
            return version | 1;
        }
        return version;
    }
    
    public static boolean decodeMutableIndexConfiguredProperly(long version) {
        return (version & 0xF) == 0;
    }

    // Given the encoded integer representing the client hbase version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodeHBaseVersion(long version) {
        return (int) (version >>> Byte.SIZE * 5);
    }

    public static String decodeHBaseVersionAsString(int version) {
        int major = (version >>> Byte.SIZE  * 2) & 0xFF;
        int minor = (version >>> Byte.SIZE  * 1) & 0xFF;
        int patch = version & 0xFF;
        return major + "." + minor + "." + patch;
    }

    public static long encodeHBaseAndPhoenixVersions(String hbaseVersion) {
        return (((long) encodeVersion(hbaseVersion)) << (Byte.SIZE * 5)) |
                (((long) encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION,
                        MetaDataProtocol.PHOENIX_PATCH_NUMBER)) << (Byte.SIZE * 1));
    }

    // Encode a version string in the format of "major.minor.patch" into an integer.
    public static int encodeVersion(String version) {
        String[] versionParts = version.split("[-\\.]");
        return encodeVersion(versionParts[0], versionParts.length > 1 ? versionParts[1] : null, versionParts.length > 2 ? versionParts[2] : null);
    }

    // Encode the major as 2nd byte in the int, minor as the first byte and patch as the last byte.
    public static int encodeVersion(String major, String minor, String patch) {
        return encodeVersion(major == null ? 0 : Integer.parseInt(major), minor == null ? 0 : Integer.parseInt(minor), 
                        patch == null ? 0 : Integer.parseInt(patch));
    }

    public static int encodeVersion(int major, int minor, int patch) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        version |= patch;
        return version;
    }

    public static int encodeMaxPatchVersion(int major, int minor) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        version |= 0xFF;
        return version;
    }

    public static int encodeMinPatchVersion(int major, int minor) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        return version;
    }

    public static void getSchemaAndTableName(List<Mutation> tableMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 2, rowKeyMetaData);
    }
    
    public static byte[] getParentTableName(List<Mutation> tableMetadata) {
        if (tableMetadata.size() == 1) {
            return null;
        }
        byte[][] rowKeyMetaData = new byte[2][];
        getSchemaAndTableName(tableMetadata, rowKeyMetaData);
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        Mutation m = getParentTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 2, rowKeyMetaData);
        if (Bytes.compareTo(tableName, rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]) == 0) {
            return null;
        }
        return rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
    }
    
    public static long getSequenceNumber(Mutation tableMutation) {
        List<KeyValue> kvs = tableMutation.getFamilyMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (KeyValue kv : kvs) { // list is not ordered, so search. TODO: we could potentially assume the position
                if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES, 0, PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES.length) == 0) {
                    return PDataType.LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), null);
                }
            }
        }
        throw new IllegalStateException();
    }
    
    public static long getSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getTableHeaderRow(tableMetaData));
    }
    
    public static long getParentSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getParentTableHeaderRow(tableMetaData));
    }
    
    public static Mutation getTableHeaderRow(List<Mutation> tableMetaData) {
        return tableMetaData.get(0);
    }

    public static Mutation getParentTableHeaderRow(List<Mutation> tableMetaData) {
        return tableMetaData.get(tableMetaData.size()-1);
    }

    public static long getClientTimeStamp(List<Mutation> tableMetadata) {
        Mutation m = tableMetadata.get(0);
        return getClientTimeStamp(m);
    }    

    public static long getClientTimeStamp(Mutation m) {
        Collection<List<KeyValue>> kvs = m.getFamilyMap().values();
        // Empty if Mutation is a Delete
        // TODO: confirm that Delete timestamp is reset like Put
        return kvs.isEmpty() ? m.getTimeStamp() : kvs.iterator().next().get(0).getTimestamp();
    }    

    public static byte[] getParentLinkKey(String schemaName, String tableName, String indexName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName), QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(indexName));
    }

    public static byte[] getParentLinkKey(byte[] schemaName, byte[] tableName, byte[] indexName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName, QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY, indexName);
    }


}
