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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SaltingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class UpgradeUtil {
    private static final Logger logger = LoggerFactory.getLogger(UpgradeUtil.class);
    private static final byte[] SEQ_PREFIX_BYTES = ByteUtil.concat(QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes("_SEQ_"));

    private UpgradeUtil() {
    }

    @SuppressWarnings("deprecation")
    public static boolean upgradeSequenceTable(PhoenixConnection conn, int nSaltBuckets) throws SQLException {
        logger.info("Upgrading SYSTEM.SEQUENCE table");

        byte[] seqTableKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.TYPE_SEQUENCE);
        HTableInterface sysTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        try {
            logger.info("Setting SALT_BUCKETS property of SYSTEM.SEQUENCE to " + SaltingUtil.MAX_BUCKET_NUM);
            KeyValue saltKV = KeyValueUtil.newKeyValue(seqTableKey, 
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES,
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                    PDataType.INTEGER.toBytes(nSaltBuckets));
            Put saltPut = new Put(seqTableKey);
            saltPut.add(saltKV);
            // Prevent multiple clients from doing this upgrade
            if (!sysTable.checkAndPut(seqTableKey,
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES, null, saltPut)) {

                logger.info("SYSTEM.SEQUENCE table has already been upgraded");
                return false;
            }

            int batchSizeBytes = 100 * 1024; // 100K chunks
            int sizeBytes = 0;
            List<Mutation> mutations =  Lists.newArrayListWithExpectedSize(10000);
    
            boolean success = false;
            Scan scan = new Scan();
            scan.setRaw(true);
            scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
            HTableInterface seqTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SEQUENCE_FULLNAME_BYTES);
            try {
                boolean committed = false;
               logger.info("Adding salt byte to all SYSTEM.SEQUENCE rows");
                ResultScanner scanner = seqTable.getScanner(scan);
                try {
                    Result result;
                     while ((result = scanner.next()) != null) {
                        for (KeyValue keyValue : result.raw()) {
                            KeyValue newKeyValue = addSaltByte(keyValue, nSaltBuckets);
                            if (newKeyValue != null) {
                                sizeBytes += newKeyValue.getLength();
                                if (KeyValue.Type.codeToType(newKeyValue.getType()) == KeyValue.Type.Put) {
                                    // Delete old value
                                    byte[] buf = keyValue.getBuffer();
                                    Delete delete = new Delete(keyValue.getRow());
                                    KeyValue deleteKeyValue = new KeyValue(buf, keyValue.getRowOffset(), keyValue.getRowLength(),
                                            buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                                            buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                                            keyValue.getTimestamp(), KeyValue.Type.Delete,
                                            ByteUtil.EMPTY_BYTE_ARRAY,0,0);
                                    delete.addDeleteMarker(deleteKeyValue);
                                    mutations.add(delete);
                                    sizeBytes += deleteKeyValue.getLength();
                                    // Put new value
                                    Put put = new Put(newKeyValue.getRow());
                                    put.add(newKeyValue);
                                    mutations.add(put);
                                } else if (KeyValue.Type.codeToType(newKeyValue.getType()) == KeyValue.Type.Delete){
                                    // Copy delete marker using new key so that it continues
                                    // to delete the key value preceding it that will be updated
                                    // as well.
                                    Delete delete = new Delete(newKeyValue.getRow());
                                    delete.addDeleteMarker(newKeyValue);
                                    mutations.add(delete);
                                }
                            }
                            if (sizeBytes >= batchSizeBytes) {
                                logger.info("Committing bactch of SYSTEM.SEQUENCE rows");
                                seqTable.batch(mutations);
                                mutations.clear();
                                sizeBytes = 0;
                                committed = true;
                            }
                        }
                    }
                    if (!mutations.isEmpty()) {
                        logger.info("Committing last bactch of SYSTEM.SEQUENCE rows");
                        seqTable.batch(mutations);
                    }
                    logger.info("Successfully completed upgrade of SYSTEM.SEQUENCE");
                    success = true;
                    return true;
                } catch (InterruptedException e) {
                    throw ServerUtil.parseServerException(e);
                } finally {
                    try {
                        scanner.close();
                    } finally {
                        if (!success) {
                            if (!committed) { // Try to recover by setting salting back to off, as we haven't successfully committed anything
                                // Don't use Delete here as we'd never be able to change it again at this timestamp.
                                KeyValue unsaltKV = KeyValueUtil.newKeyValue(seqTableKey, 
                                        PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                                        PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES,
                                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                                        PDataType.INTEGER.toBytes(0));
                                Put unsaltPut = new Put(seqTableKey);
                                unsaltPut.add(unsaltKV);
                                try {
                                    sysTable.put(unsaltPut);
                                    success = true;
                                } finally {
                                    if (!success) logger.error("SYSTEM.SEQUENCE TABLE LEFT IN CORRUPT STATE");
                                }
                            } else { // We're screwed b/c we've already committed some salted sequences...
                                logger.error("SYSTEM.SEQUENCE TABLE LEFT IN CORRUPT STATE");
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw ServerUtil.parseServerException(e);
            } finally {
                try {
                    seqTable.close();
                } catch (IOException e) {
                    logger.warn("Exception during close",e);
                }
            }
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        } finally {
            try {
                sysTable.close();
            } catch (IOException e) {
                logger.warn("Exception during close",e);
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    private static KeyValue addSaltByte(KeyValue keyValue, int nSaltBuckets) {
        byte[] buf = keyValue.getBuffer();
        int length = keyValue.getRowLength();
        int offset = keyValue.getRowOffset();
        boolean isViewSeq = length > SEQ_PREFIX_BYTES.length && Bytes.compareTo(SEQ_PREFIX_BYTES, 0, SEQ_PREFIX_BYTES.length, buf, offset, SEQ_PREFIX_BYTES.length) == 0;
        if (!isViewSeq && nSaltBuckets == 0) {
            return null;
        }
        byte[] newBuf;
        if (isViewSeq) { // We messed up the name for the sequences for view indexes so we'll take this opportunity to fix it
            if (buf[length-1] == 0) { // Global indexes on views have trailing null byte
                length--;
            }
            byte[][] rowKeyMetaData = new byte[3][];
            SchemaUtil.getVarChars(buf, offset, length, 0, rowKeyMetaData);
            byte[] schemaName = rowKeyMetaData[PhoenixDatabaseMetaData.SCHEMA_NAME_INDEX];
            byte[] unprefixedSchemaName = new byte[schemaName.length - MetaDataUtil.VIEW_INDEX_SEQUENCE_PREFIX_BYTES.length];
            System.arraycopy(schemaName, MetaDataUtil.VIEW_INDEX_SEQUENCE_PREFIX_BYTES.length, unprefixedSchemaName, 0, unprefixedSchemaName.length);
            byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
            PName physicalName = PNameFactory.newName(unprefixedSchemaName);
            // Reformulate key based on correct data
            newBuf = MetaDataUtil.getViewIndexSequenceKey(tableName == null ? null : Bytes.toString(tableName), physicalName, nSaltBuckets).getKey();
        } else {
            newBuf = new byte[length + 1];
            System.arraycopy(buf, offset, newBuf, SaltingUtil.NUM_SALTING_BYTES, length);
            newBuf[0] = SaltingUtil.getSaltingByte(newBuf, SaltingUtil.NUM_SALTING_BYTES, length, nSaltBuckets);
        }
        return new KeyValue(newBuf, 0, newBuf.length,
                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                keyValue.getTimestamp(), KeyValue.Type.codeToType(keyValue.getType()),
                buf, keyValue.getValueOffset(), keyValue.getValueLength());
    }

}
