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
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.SaltingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class UpgradeUtil {
    private static final Logger logger = LoggerFactory.getLogger(UpgradeUtil.class);

    private UpgradeUtil() {
    }

    public static boolean addSaltByteToSequenceTable(PhoenixConnection conn) throws SQLException {
        logger.info("Upgrading SYSTEM.SEQUENCE table");

        HTableInterface sysTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES);
        try {
            byte[] seqTableKey = SchemaUtil.getTableKey(null, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.TYPE_SEQUENCE);
            logger.info("Setting SALT_BUCKETS property of SYSTEM.SEQUENCE to " + SaltingUtil.MAX_BUCKET_NUM);
            KeyValue saltKV = KeyValueUtil.newKeyValue(seqTableKey, 
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES,
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                    PDataType.INTEGER.toBytes(SaltingUtil.MAX_BUCKET_NUM));
            Put put = new Put(seqTableKey);
            put.add(saltKV);
            // Prevent multiple clients from doing this upgrade
            if (!sysTable.checkAndPut(seqTableKey,
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.SALT_BUCKETS_BYTES, null, put)) {

                logger.info("SYSTEM.SEQUENCE table has already been upgraded");
                return false;
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
        int batchSizeBytes = 100 * 1024; // 100K chunks
        int sizeBytes = 0;
        List<Mutation> mutations =  Lists.newArrayListWithExpectedSize(10000);

        boolean success = false;
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setMaxVersions(MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS);
        HTableInterface seqTable = conn.getQueryServices().getTable(PhoenixDatabaseMetaData.SEQUENCE_TABLE_NAME_BYTES);
        try {
            logger.info("Adding salt byte to all SYSTEM.SEQUENCE rows");
            ResultScanner scanner = seqTable.getScanner(scan);
            try {
                Result result;
                while ((result = scanner.next()) != null) {
                    for (KeyValue keyValue : result.raw()) {
                        KeyValue newKeyValue = addSaltByte(keyValue);
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
                        if (sizeBytes >= batchSizeBytes) {
                            logger.info("Committing bactch of SYSTEM.SEQUENCE rows");
                            seqTable.batch(mutations);
                            mutations.clear();
                            sizeBytes = 0;
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
                if (!success) logger.error("SYSTEM.SEQUENCE TABLE LEFT IN CORRUPT STATE");
                scanner.close();
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
    }
    
    private static KeyValue addSaltByte(KeyValue keyValue) {
        int length = keyValue.getRowLength();
        int offset = keyValue.getRowOffset();
        byte[] buf = keyValue.getBuffer();
        byte[] newBuf = new byte[length + 1];
        System.arraycopy(buf, offset, newBuf, SaltingUtil.NUM_SALTING_BYTES, length);
        newBuf[0] = SaltingUtil.getSaltingByte(newBuf, SaltingUtil.NUM_SALTING_BYTES, length, SaltingUtil.MAX_BUCKET_NUM);
        return new KeyValue(newBuf, 0, newBuf.length,
                buf, keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                buf, keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
                keyValue.getTimestamp(), KeyValue.Type.codeToType(keyValue.getType()),
                buf, keyValue.getValueOffset(), keyValue.getValueLength());
    }

}
