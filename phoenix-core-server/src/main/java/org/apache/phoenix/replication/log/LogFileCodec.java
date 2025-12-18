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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * Default Codec for encoding and decoding ReplicationLog Records within a block buffer. This
 * implementation uses standard Java DataInput/DataOutput for serialization. Record Format within a
 * block:
 *
 * <pre>
 *   +--------------------------------------------+
 *   | RECORD LENGTH (vint)                       |
 *   +--------------------------------------------+
 *   | RECORD HEADER                              |
 *   |  - Mutation type (byte)                    |
 *   |  - HBase table name length (vint)          |
 *   |  - HBase table name (byte[])               |
 *   |  - Transaction/SCN or commit ID (vint)     |
 *   +--------------------------------------------+
 *   | ROW KEY LENGTH (vint)                      |
 *   | ROW KEY (byte[])                           |
 *   +--------------------------------------------+
 *   | MUTATION TIMESTAMP (vint)                  |
 *   +--------------------------------------------+
 *   | NUMBER OF COLUMN FAMILIES CHANGED (vint)   |
 *   +--------------------------------------------+
 *   | PER-FAMILY DATA (repeated)                 |
 *   |   +--------------------------------------+ |
 *   |   | COLUMN FAMILY NAME LENGTH (vint)     | |
 *   |   | COLUMN FAMILY NAME (byte[])          | |
 *   |   | NUMBER OF CELLS IN FAMILY (vint)     | |
 *   |   +--------------------------------------+ |
 *   |   | PER-CELL DATA (repeated)             | |
 *   |   |   +––––––––––––----------------–--–+ | |
 *   |   |   | COLUMN QUALIFIER LENGTH (vint) | | |
 *   |   |   | COLUMN QUALIFIER (byte[])      | | |
 *   |   |   | VALUE LENGTH (vint)            | | |
 *   |   |   | VALUE (byte[])                 | | |
 *   |   |   +–––––––––––––--------------––--–+ | |
 *   |   +--------------------------------------+ |
 *   +--------------------------------------------+
 * </pre>
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

      Mutation mutation = record.getMutation();
      LogFileRecord.MutationType mutationType = LogFileRecord.MutationType.get(mutation);
      recordOut.writeByte(mutationType.getCode());
      byte[] nameBytes = record.getHBaseTableName().getBytes(StandardCharsets.UTF_8);
      WritableUtils.writeVInt(recordOut, nameBytes.length);
      recordOut.write(nameBytes);
      WritableUtils.writeVLong(recordOut, record.getCommitId());
      byte[] rowKey = mutation.getRow();
      WritableUtils.writeVInt(recordOut, rowKey.length);
      recordOut.write(rowKey);
      recordOut.writeLong(mutation.getTimestamp());

      Map<byte[], List<Cell>> familyMap = mutation.getFamilyCellMap();
      int cfCount = familyMap.size();
      WritableUtils.writeVInt(recordOut, cfCount);

      for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
        byte[] cf = entry.getKey();
        WritableUtils.writeVInt(recordOut, cf.length);
        recordOut.write(cf);
        List<Cell> cells = entry.getValue();
        WritableUtils.writeVInt(recordOut, cells.size());
        for (Cell cell : cells) {
          WritableUtils.writeVInt(recordOut, cell.getQualifierLength());
          recordOut.write(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength());
          WritableUtils.writeVInt(recordOut, cell.getValueLength());
          if (cell.getValueLength() > 0) {
            recordOut.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
          }
        }
      }

      byte[] currentRecordBytes = currentRecord.toByteArray();
      // Write total record length
      WritableUtils.writeVInt(out, currentRecordBytes.length);
      // Write the record
      out.write(currentRecordBytes);

      // Set the size (including the vint prefix) on the record object
      ((LogFileRecord) record).setSerializedLength(
        currentRecordBytes.length + WritableUtils.getVIntSize(currentRecordBytes.length));

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
    public boolean advance() throws IOException {
      try {
        int recordDataLength = WritableUtils.readVInt(in);
        recordDataLength += WritableUtils.getVIntSize(recordDataLength);

        current = new LogFileRecord();
        // Set the total serialized length on the record
        current.setSerializedLength(recordDataLength);

        LogFileRecord.MutationType type = LogFileRecord.MutationType.codeToType(in.readByte());

        int nameBytesLen = WritableUtils.readVInt(in);
        byte[] nameBytes = new byte[nameBytesLen];
        in.readFully(nameBytes);

        current.setHBaseTableName(Bytes.toString(nameBytes));

        current.setCommitId(WritableUtils.readVLong(in));

        int rowKeyLen = WritableUtils.readVInt(in);
        byte[] rowKey = new byte[rowKeyLen];
        in.readFully(rowKey);

        Mutation mutation;
        switch (type) {
          case PUT:
            mutation = new Put(rowKey);
            break;
          case DELETE:
          case DELETEFAMILYVERSION:
          case DELETECOLUMN:
          case DELETEFAMILY:
            mutation = new Delete(rowKey);
            break;
          default:
            throw new UnsupportedOperationException("Unhandled mutation type " + type);
        }
        current.setMutation(mutation);

        long ts = in.readLong();
        mutation.setTimestamp(ts);

        int cfCount = WritableUtils.readVInt(in);
        for (int i = 0; i < cfCount; i++) {
          // Col name
          int cfLen = WritableUtils.readVInt(in);
          byte[] cf = new byte[cfLen];
          in.readFully(cf);
          // Qualifiers+Values Count
          int columnValuePairsCount = WritableUtils.readVInt(in);
          for (int j = 0; j < columnValuePairsCount; j++) {
            // Qualifier name
            int qualLen = WritableUtils.readVInt(in);
            byte[] qual = new byte[qualLen];
            if (qualLen > 0) {
              in.readFully(qual);
            }
            // Value
            int valueLen = WritableUtils.readVInt(in);
            byte[] value = new byte[valueLen];
            if (valueLen > 0) {
              in.readFully(value);
            }
            switch (type) {
              case PUT:
                ((Put) mutation).addColumn(cf, qual, ts, value);
                break;
              case DELETE:
              case DELETECOLUMN:
                ((Delete) mutation).addColumn(cf, qual, ts);
                break;
              case DELETEFAMILYVERSION:
                ((Delete) mutation).addFamilyVersion(cf, ts);
                break;
              case DELETEFAMILY:
                ((Delete) mutation).addFamily(cf);
                break;
              default:
                throw new UnsupportedOperationException("Unhandled mutation type " + type);
            }
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
