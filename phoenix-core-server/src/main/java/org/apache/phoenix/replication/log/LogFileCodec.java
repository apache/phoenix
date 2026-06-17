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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ByteBuffInputStream;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * Default Codec for encoding and decoding ReplicationLog Records within a block buffer. The on-disk
 * record is cell-oriented (mirroring HBase's WALEdit): a record carries a flat ordered list of
 * cells across one or more rows. Mutation reconstruction (grouping by row+type) is the consumer's
 * responsibility and lives in {@link org.apache.phoenix.replication.MutationCellGrouper}.
 *
 * <pre>
 *   +--------------------------------------------+
 *   | RECORD LENGTH (vint)                       |
 *   +--------------------------------------------+
 *   | RECORD HEADER                              |
 *   |  - HBase table name length (vint)          |
 *   |  - HBase table name (byte[])               |
 *   |  - Commit ID (vlong)                       |
 *   +--------------------------------------------+
 *   | NUMBER OF ATTRIBUTES (vint)                |
 *   +--------------------------------------------+
 *   | PER-ATTRIBUTE (repeated)                   |
 *   |   +--------------------------------------+ |
 *   |   | ATTRIBUTE KEY LENGTH (vint)          | |
 *   |   | ATTRIBUTE KEY (byte[])               | |
 *   |   | ATTRIBUTE VALUE LENGTH (vint)        | |
 *   |   | ATTRIBUTE VALUE (byte[])             | |
 *   |   +--------------------------------------+ |
 *   +--------------------------------------------+
 *   | NUMBER OF CELLS (vint)                     |
 *   +--------------------------------------------+
 *   | PER-CELL DATA (repeated)                   |
 *   |   +--------------------------------------+ |
 *   |   | ROW LENGTH (vint)                    | |
 *   |   | ROW (byte[])                         | |
 *   |   | FAMILY LENGTH (vint)                 | |
 *   |   | FAMILY (byte[])                      | |
 *   |   | QUALIFIER LENGTH (vint)              | |
 *   |   | QUALIFIER (byte[])                   | |
 *   |   | TIMESTAMP (long)                     | |
 *   |   | TYPE (byte)                          | |
 *   |   | VALUE LENGTH (vint)                  | |
 *   |   | VALUE (byte[])                       | |
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
      if (record.getCells().isEmpty()) {
        throw new IllegalArgumentException("Cannot encode a record with no cells");
      }
      DataOutput recordOut = new DataOutputStream(currentRecord);

      // Header: table name + commit id
      byte[] nameBytes = record.getHBaseTableName().getBytes(StandardCharsets.UTF_8);
      WritableUtils.writeVInt(recordOut, nameBytes.length);
      recordOut.write(nameBytes);
      WritableUtils.writeVLong(recordOut, record.getCommitId());

      // Attributes
      Map<String, byte[]> attrs = record.getAttributes();
      WritableUtils.writeVInt(recordOut, attrs.size());
      for (Map.Entry<String, byte[]> e : attrs.entrySet()) {
        byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
        WritableUtils.writeVInt(recordOut, keyBytes.length);
        recordOut.write(keyBytes);
        byte[] val = e.getValue();
        WritableUtils.writeVInt(recordOut, val.length);
        if (val.length > 0) {
          recordOut.write(val);
        }
      }

      // Cells
      List<Cell> cells = record.getCells();
      WritableUtils.writeVInt(recordOut, cells.size());
      for (Cell cell : cells) {
        WritableUtils.writeVInt(recordOut, cell.getRowLength());
        recordOut.write(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        WritableUtils.writeVInt(recordOut, cell.getFamilyLength());
        recordOut.write(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
        WritableUtils.writeVInt(recordOut, cell.getQualifierLength());
        if (cell.getQualifierLength() > 0) {
          recordOut.write(cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength());
        }
        recordOut.writeLong(cell.getTimestamp());
        recordOut.writeByte(cell.getTypeByte());
        WritableUtils.writeVInt(recordOut, cell.getValueLength());
        if (cell.getValueLength() > 0) {
          recordOut.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        }
      }

      byte[] currentRecordBytes = currentRecord.toByteArray();
      WritableUtils.writeVInt(out, currentRecordBytes.length);
      out.write(currentRecordBytes);

      ((LogFileRecord) record).setSerializedLength(
        currentRecordBytes.length + WritableUtils.getVIntSize(currentRecordBytes.length));

      currentRecord.reset();
    }
  }

  private static class RecordDecoder implements LogFile.Codec.Decoder {
    private final DataInput in;
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
        current.setSerializedLength(recordDataLength);

        // Header
        int nameBytesLen = WritableUtils.readVInt(in);
        byte[] nameBytes = new byte[nameBytesLen];
        in.readFully(nameBytes);
        current.setHBaseTableName(Bytes.toString(nameBytes));
        current.setCommitId(WritableUtils.readVLong(in));

        // Attributes
        int attrCount = WritableUtils.readVInt(in);
        Map<String, byte[]> attrs = attrCount == 0 ? new HashMap<>() : new HashMap<>(attrCount);
        for (int i = 0; i < attrCount; i++) {
          int keyLen = WritableUtils.readVInt(in);
          byte[] keyBytes = new byte[keyLen];
          in.readFully(keyBytes);
          int valLen = WritableUtils.readVInt(in);
          byte[] valBytes = new byte[valLen];
          if (valLen > 0) {
            in.readFully(valBytes);
          }
          attrs.put(new String(keyBytes, StandardCharsets.UTF_8), valBytes);
        }
        current.setAttributes(attrs);

        // Cells
        int cellCount = WritableUtils.readVInt(in);
        List<Cell> cells = new ArrayList<>(cellCount);
        for (int i = 0; i < cellCount; i++) {
          int rowLen = WritableUtils.readVInt(in);
          byte[] row = new byte[rowLen];
          if (rowLen > 0) {
            in.readFully(row);
          }
          int famLen = WritableUtils.readVInt(in);
          byte[] family = new byte[famLen];
          if (famLen > 0) {
            in.readFully(family);
          }
          int qualLen = WritableUtils.readVInt(in);
          byte[] qual = new byte[qualLen];
          if (qualLen > 0) {
            in.readFully(qual);
          }
          long ts = in.readLong();
          byte typeByte = in.readByte();
          int valueLen = WritableUtils.readVInt(in);
          byte[] value = new byte[valueLen];
          if (valueLen > 0) {
            in.readFully(value);
          }
          cells.add(new KeyValue(row, 0, row.length, family, 0, family.length, qual, 0, qual.length,
            ts, KeyValue.Type.codeToType(typeByte), value, 0, value.length));
        }
        current.setCells(cells);

        return true;
      } catch (EOFException e) {
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
