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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public interface LogFileTestUtil {

    static LogFile.Record newPutRecord(String table, long commitId, String rowKey, long ts,
            int numCols) {
        return new LogFileRecord().setMutation(newPut(rowKey, ts, numCols))
            .setHBaseTableName(table).setCommitId(commitId);
    }

    static Put newPut(String rowKey, long ts, int numCols) {
        byte[] qualifier = Bytes.toBytes("q");
        Put put = new Put(Bytes.toBytes(rowKey));
        put.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            put.addColumn(Bytes.toBytes("col" + i), qualifier, ts,
                Bytes.toBytes("v" + i + "_" + rowKey));
        }
        return put;
    }

    static LogFile.Record newDeleteRecord(String table, long commitId, String rowKey, long ts,
            int numCols) {
        return new LogFileRecord().setMutation(newDelete(rowKey, ts, numCols))
            .setHBaseTableName(table).setCommitId(commitId);
    }

    static Delete newDelete(String rowKey, long ts, int numCols) {
        byte[] qualifier = Bytes.toBytes("q");
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            delete.addColumn(Bytes.toBytes("col" + i), qualifier);
        }
        return delete;
    }

    static LogFile.Record newDeleteColumnRecord(String table, long commitId, String rowKey,
            long ts, int numCols) throws IOException {
        byte[] qualifier = Bytes.toBytes("q");
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            byte[] column = Bytes.toBytes("col" + i);
            delete.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(Bytes.toBytes(rowKey)).setFamily(column).setQualifier(qualifier)
                .setTimestamp(ts).setType(Cell.Type.DeleteColumn).build());
        }
        LogFile.Record record = new LogFileRecord().setMutation(delete).setHBaseTableName(table)
            .setCommitId(commitId);
        return record;
    }

    static LogFile.Record newDeleteFamilyRecord(String table, long commitId, String rowKey,
            long ts, int numCols) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            byte[] column = Bytes.toBytes("col" + i);
            delete.addFamily(column);
        }
        LogFile.Record record = new LogFileRecord().setMutation(delete).setHBaseTableName(table)
            .setCommitId(commitId);
        return record;
    }

    static LogFile.Record newDeleteFamilyVersionRecord(String table, long commitId, String rowKey,
            long ts, int numCols) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            byte[] column = Bytes.toBytes("col" + i);
            delete.addFamilyVersion(column, ts);
        }
        LogFile.Record record = new LogFileRecord().setMutation(delete).setHBaseTableName(table)
            .setCommitId(commitId);
        return record;
    }

    static void assertRecordEquals(String message, LogFile.Record r1, LogFile.Record r2)
            throws AssertionError {
        try {
            if (!r1.getMutation().toJSON().equals(r2.getMutation().toJSON())
                    || !r1.getHBaseTableName().equals(r2.getHBaseTableName())
                    || r1.getCommitId() != r2.getCommitId()) {
                throw new AssertionError(message + ": left=" + r1 + ", right=" + r2);
            }
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    static void assertMutationEquals(String message, Mutation m1, Mutation m2) {
        try {
            if (!m1.toJSON().equals(m2.toJSON())) {
                throw new AssertionError(message + ": left=" + m1 + ", right=" + m2);
            }
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    static class SeekableByteArrayInputStream extends ByteArrayInputStream
            implements SeekableDataInput {

        // A view of ourselves as a data input stream
        private DataInputStream stream;

        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
            this.stream = new DataInputStream(this);
        }

        public SeekableByteArrayInputStream(byte[] buf, int offset, int length) {
            super(buf, offset, length);
            this.stream = new DataInputStream(this);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0) {
                throw new IOException("Cannot seek to negative position");
            }
            if (pos > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Cannot seek beyond Integer.MAX_VALUE");
            }
            if (pos >= count) {
                this.pos = count; // Position at the end
                if (pos > count) {
                    throw new EOFException("Seek position is past the end of the stream");
                }
            } else {
                this.pos = (int) pos;
            }
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long pos) throws IOException {
            seek(pos);
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            if (buffer == null) {
                throw new NullPointerException();
            }
            if (offset < 0 || length < 0 || offset + length > buffer.length) {
                throw new IndexOutOfBoundsException(String.format(
                    "Offset %d or length %d is invalid for buffer of size %d", offset, length,
                    buffer.length));
            }
            if (position < 0) {
                throw new IOException("Position cannot be negative");
            }
            if (position > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Position cannot exceed Integer.MAX_VALUE");
            }
            if (length == 0) {
                return 0;
            }
            int pos = (int) position;
            if (pos >= count) {
                return -1;
            }
            int avail = count - pos;
            int n = Math.min(length, avail);
            System.arraycopy(buf, pos, buffer, offset, n);
            return n;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException {
            if (buffer == null) {
                throw new NullPointerException("Buffer cannot be null");
            }
            if (offset < 0 || length < 0 || offset + length > buffer.length) {
                throw new IndexOutOfBoundsException(String.format(
                    "Offset %d or length %d is invalid for buffer of size %d", offset, length,
                    buffer.length));
            }
            if (position < 0) {
                throw new IOException("Position cannot be negative");
            }
            if (position > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Position cannot exceed Integer.MAX_VALUE");
            }
            if (length == 0) {
                return;
            }
            int pos = (int) position;
            if (pos >= count) {
                throw new EOFException("Read position is at or past the end of the stream");
            }
            int avail = count - pos;
            if (length > avail) {
                throw new EOFException(String.format(
                    "Premature EOF from stream: expected %d bytes, but only %d available", length,
                    avail));
            }
            System.arraycopy(buf, pos, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            stream.readFully(b);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            stream.readFully(b, off, len);
        }

        @Override
        public int skipBytes(int n) throws IOException {
            return stream.skipBytes(n);
        }

        @Override
        public boolean readBoolean() throws IOException {
            return stream.readBoolean();
        }

        @Override
        public byte readByte() throws IOException {
            return stream.readByte();
        }

        @Override
        public int readUnsignedByte() throws IOException {
            return stream.readUnsignedByte();
        }

        @Override
        public short readShort() throws IOException {
            return stream.readShort();
        }

        @Override
        public int readUnsignedShort() throws IOException {
            return stream.readUnsignedShort();
        }

        @Override
        public char readChar() throws IOException {
            return stream.readChar();
        }

        @Override
        public int readInt() throws IOException {
            return stream.readInt();
        }

        @Override
        public long readLong() throws IOException {
            return stream.readLong();
        }

        @Override
        public float readFloat() throws IOException {
            return stream.readFloat();
        }

        @Override
        public double readDouble() throws IOException {
            return stream.readDouble();
        }

        @Override
        @Deprecated
        public String readLine() throws IOException {
            return stream.readLine();
        }

        @Override
        public String readUTF() throws IOException {
            return stream.readUTF();
        }

    }

    static class SyncableByteArrayOutputStream extends OutputStream
          implements SyncableDataOutput {

        private ByteArrayOutputStream out = new ByteArrayOutputStream();
        // A view of ourselves as a data output stream
        private DataOutputStream stream;

        public SyncableByteArrayOutputStream() {
            this.stream = new DataOutputStream(out);
        }

        @Override
        public void writeBoolean(boolean v) throws IOException {
            stream.writeBoolean(v);;
        }

        @Override
        public void writeByte(int v) throws IOException {
            stream.writeByte(v);
        }

        @Override
        public void writeShort(int v) throws IOException {
            stream.writeShort(v);
        }

        @Override
        public void writeChar(int v) throws IOException {
            stream.writeChar(v);
        }

        @Override
        public void writeInt(int v) throws IOException {
            stream.writeInt(v);
        }

        @Override
        public void writeLong(long v) throws IOException {
            stream.writeLong(v);
        }

        @Override
        public void writeFloat(float v) throws IOException {
            stream.writeFloat(v);
        }

        @Override
        public void writeDouble(double v) throws IOException {
            stream.writeDouble(v);
        }

        @Override
        public void writeBytes(String s) throws IOException {
            stream.writeBytes(s);
        }

        @Override
        public void writeChars(String s) throws IOException {
            stream.writeChars(s);
        }

        @Override
        public void writeUTF(String s) throws IOException {
            stream.writeUTF(s);
        }

        @Override
        public long getPos() throws IOException {
            return out.size();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void hflush() throws IOException {

        }

        @Override
        public void hsync() throws IOException {

        }

        @Override
        public void sync() throws IOException {

        }

    }

}
