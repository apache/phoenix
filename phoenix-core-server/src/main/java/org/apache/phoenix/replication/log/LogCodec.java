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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
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
 * Record Format within a block (variable length encoding used where efficient):
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
public class LogCodec implements Log.Codec {

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

    private static class RecordEncoder implements Log.Codec.Encoder {
        private final DataOutput out;

        RecordEncoder(DataOutput out) {
            this.out = out;
        }

        @Override
        public void write(Log.Record record) throws IOException {
            // Calculate serialized size
            int size = calculateRecordSize(record);

            // Set the calculated size (including the vint prefix) on the record object
            ((LogRecord) record).setSerializedLength(WritableUtils.getVIntSize(size) + size);

            // Write total record length
            WritableUtils.writeVInt(out, size);

            // Write record fields
            out.writeByte(record.getMutationType().getCode());
            WritableUtils.writeString(out, record.getSchemaObjectName()); // Uses vint for length
            WritableUtils.writeVLong(out, record.getCommitId());
            WritableUtils.writeVInt(out, record.getRowKey().length);
            out.write(record.getRowKey());
            out.writeLong(record.getTimestamp());

            int colCount = record.getColumnCount();
            WritableUtils.writeVInt(out, colCount);

            if (colCount > 0) {
                for (Map.Entry<byte[], byte[]> entry : record.getColumnValues()) {
                    byte[] colName = entry.getKey();
                    byte[] colValue = entry.getValue();
                    WritableUtils.writeVInt(out, colName.length);
                    out.write(colName);
                    WritableUtils.writeVInt(out, colValue.length);
                    out.write(colValue);
                }
            }
        }

        private int calculateRecordSize(Log.Record record) {
            int size = 0;
            size += Bytes.SIZEOF_BYTE; // Mutation Type
            size += WritableUtils.getVIntSize(record.getSchemaObjectName().length())
                + record.getSchemaObjectName().getBytes(StandardCharsets.UTF_8).length;
            size += WritableUtils.getVIntSize(record.getCommitId()); // Commit ID
            size += WritableUtils.getVIntSize(record.getRowKey().length)
                + record.getRowKey().length; // Row Key
            size += Bytes.SIZEOF_LONG; // Timestamp
            size += WritableUtils.getVIntSize(record.getColumnCount()); // Column Count
            if (record.getColumnCount() > 0) {
                for (Map.Entry<byte[], byte[]> entry : record.getColumnValues()) {
                    size += WritableUtils.getVIntSize(entry.getKey().length)
                        + entry.getKey().length; // Col Name
                    size += WritableUtils.getVIntSize(entry.getValue().length)
                        + entry.getValue().length; // Col Value
                }
            }
            return size;
        }
    }

    private static class RecordDecoder implements Log.Codec.Decoder {
        private final DataInput in;
        // A reference to the object populated by the last successful advance()
        private LogRecord current = null;

        RecordDecoder(DataInput in) {
            this.in = in;
        }

        @Override
        public boolean advance(Log.Record reuse) throws IOException {
            try {
                int recordDataLength = WritableUtils.readVInt(in);
                recordDataLength += WritableUtils.getVIntSize(recordDataLength);

                // If we are reusing a record object, prepare it for new data
                if (reuse == null || !(reuse instanceof LogRecord)) {
                    current = new LogRecord();
                } else {
                    current = (LogRecord) reuse;
                    current.clearColumnValues(); // Reset collections
                }
                // Set the total serialized length on the record
                current.setSerializedLength(recordDataLength);
                current.setMutationType(Log.MutationType.codeToType(in.readByte()));
                current.setSchemaObjectName(WritableUtils.readString(in));
                current.setCommitId(WritableUtils.readVLong(in));

                int rowKeyLen = WritableUtils.readVInt(in);
                byte[] rowKey = new byte[rowKeyLen];
                in.readFully(rowKey);
                current.setRowKey(rowKey);

                current.setTimestamp(in.readLong());

                int colCount = WritableUtils.readVInt(in);
                for (int i = 0; i < colCount; i++) {
                    int colNameLen = WritableUtils.readVInt(in);
                    byte[] colName = new byte[colNameLen];
                    in.readFully(colName);

                    int valueLen = WritableUtils.readVInt(in);
                    byte[] value = new byte[valueLen];
                    in.readFully(value);

                    current.addColumnValue(colName, value);
                }

                return true; // Successfully read a record
            } catch (EOFException e) {
                // End of stream
                current = null;
                return false;
            }
        }

        @Override
        public Log.Record current() {
            if (current == null) {
                throw new IllegalStateException("Call advance() first or end of stream reached");
            }
            return current;
        }
    }

}
