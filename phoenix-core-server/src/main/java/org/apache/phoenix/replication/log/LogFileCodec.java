/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.log;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * Default Codec for encoding and decoding ReplicationLog Records within a block buffer.
 * This implementation uses standard Java DataInput/DataOutput for serialization.
 *
 * Record Format within a block:
 * - Total Record Length (vint)
 * - Mutation Type (byte)
 * - Schema Object Name Length (vint)
 * - Schema Object Name Bytes (byte[])
 * - Commit ID / SCN (vlong)
 * - Row Key Length (vint)
 * - Row Key Bytes (byte[])
 * - Timestamp (long)
 * - Number of Columns (vint)
 * - For each column:
 *   - Column Name Length (vint)
 *   - Column Name Bytes (byte[])
 *   - Value Length (vint)
 *   - Value Bytes (byte[])
 */
public class LogFileCodec implements LogFile.Codec {

    @Override
    public Encoder getEncoder(DataOutput out) {
        return new RecordEncoder(out);
    }

    @Override
    public Decoder getDecoder(DataInput in) {
        return new RecordDecoder(in);
    }

    @Override
    public Decoder getDecoder(ByteBuffer buffer) {
        // We use HBase ByteBuff and ByteBuffInputStream which avoids copying, because our buffers
        // are known to be heap based.
        ByteBuff wrapByteBuf = ByteBuff.wrap(buffer);
        try {
            return new RecordDecoder(new DataInputStream(new ByteBuffInputStream(wrapByteBuf)));
        } finally {
            wrapByteBuf.release();
        }
    }

    private static class RecordEncoder implements LogFile.Codec.Encoder {
        private final DataOutput out;
        private final ByteArrayOutputStream currentRecord;

        RecordEncoder(DataOutput out) {
            this.out = out;
            currentRecord = new ByteArrayOutputStream();
        }


        @Override
        public void write(LogFile.Record record) throws IOException {

            DataOutput recordOut = new DataOutputStream(currentRecord);

            // Write record fields
            recordOut.writeByte(record.getMutationType().getCode());
            byte[] nameBytes = record.getSchemaObjectName().getBytes(StandardCharsets.UTF_8);
            WritableUtils.writeVInt(recordOut, nameBytes.length);
            recordOut.write(nameBytes);
            WritableUtils.writeVLong(recordOut, record.getCommitId());
            WritableUtils.writeVInt(recordOut, record.getRowKey().length);
            recordOut.write(record.getRowKey());
            recordOut.writeLong(record.getTimestamp());

            int colCount = record.getColumnCount();
            WritableUtils.writeVInt(recordOut, colCount);

            if (colCount > 0) {
                for (Map.Entry<byte[], Map<byte[], byte[]>> entry : record.getColumnValues()) {
                    byte[] colName = entry.getKey();
                    Map<byte[], byte[]> column = entry.getValue();
                    WritableUtils.writeVInt(recordOut, colName.length);
                    recordOut.write(colName);
                    WritableUtils.writeVInt(recordOut, column.size());
                    for (Map.Entry<byte[],byte[]> qualValue : column.entrySet()) {
                        byte[] qualName = qualValue.getKey();
                        byte[] value = qualValue.getValue();
                        WritableUtils.writeVInt(recordOut, qualName.length);
                        recordOut.write(qualName);
                        WritableUtils.writeVInt(recordOut, value.length);
                        recordOut.write(value);
                    }
                }
            }

            byte[] currentRecordBytes = currentRecord.toByteArray();
            // Write total record length
            WritableUtils.writeVInt(out, currentRecordBytes.length);
            // Write the record
            out.write(currentRecordBytes);

            // Set the size (including the vint prefix) on the record object
            ((LogFileRecord) record).setSerializedLength(currentRecordBytes.length +
                WritableUtils.getVIntSize(currentRecordBytes.length));

            // Reset the ByteArrayOutputStream to release resources
            currentRecord.reset();
        }
    }

    private static class RecordDecoder implements LogFile.Codec.Decoder {
        private final DataInput in;
        // A reference to the object populated by the last successful advance()
        private LogFileRecord current = null;

        RecordDecoder(DataInput in) {
            this.in = in;
        }

        @Override
        public boolean advance(LogFile.Record reuse) throws IOException {
            try {
                int recordDataLength = WritableUtils.readVInt(in);
                recordDataLength += WritableUtils.getVIntSize(recordDataLength);

                // If we are reusing a record object, prepare it for new data
                if (reuse == null || !(reuse instanceof LogFileRecord)) {
                    current = new LogFileRecord();
                } else {
                    current = (LogFileRecord) reuse;
                    current.clearColumnValues(); // Reset collections
                }
                // Set the total serialized length on the record
                current.setSerializedLength(recordDataLength);
                current.setMutationType(LogFile.MutationType.codeToType(in.readByte()));

                int nameBytesLen = WritableUtils.readVInt(in);
                byte nameBytes[] = new byte[nameBytesLen];
                in.readFully(nameBytes);
                current.setSchemaObjectName(Bytes.toString(nameBytes));

                current.setCommitId(WritableUtils.readVLong(in));

                int rowKeyLen = WritableUtils.readVInt(in);
                byte[] rowKey = new byte[rowKeyLen];
                in.readFully(rowKey);
                current.setRowKey(rowKey);

                current.setTimestamp(in.readLong());

                int colCount = WritableUtils.readVInt(in);
                for (int i = 0; i < colCount; i++) {
                    // Col name
                    int colNameLen = WritableUtils.readVInt(in);
                    byte[] colName = new byte[colNameLen];
                    in.readFully(colName);
                    // Qualifiers+Values Count
                    int qualValuesCount = WritableUtils.readVInt(in);
                    for (int j = 0; j < qualValuesCount; j++) {
                        // Qualifier name
                        int qualNameLen = WritableUtils.readVInt(in);
                        byte[] qualName = new byte[qualNameLen];
                        in.readFully(qualName);
                        // Value
                        int valueLen = WritableUtils.readVInt(in);
                        byte[] value = new byte[valueLen];
                        in.readFully(value);
                        current.addColumnValue(colName, qualName, value);
                    }
                }

                // Successfully read a record
                return true;
            } catch (EOFException e) {
                // End of stream
                current = null;
                return false;
            }
        }

        @Override
        public LogFile.Record current() {
            if (current == null) {
                throw new IllegalStateException("Call advance() first, or end of stream reached");
            }
            return current;
        }
    }

}
