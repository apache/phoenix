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
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

public class LogBlockHeader implements Log.BlockHeader {
    private int version;
    private Compression.Algorithm compression;
    private int uncompressedSize;
    private int compressedSize;

    public static final int HEADER_SIZE = Log.BlockHeader.MAGIC.length + (2 * Bytes.SIZEOF_BYTE)
        + (2 * Bytes.SIZEOF_INT);

    public LogBlockHeader() {
        this.version = Log.BlockHeader.VERSION;
        this.compression = Compression.Algorithm.NONE;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public Compression.Algorithm getCompression() {
        return compression;
    }

    @Override
    public Log.BlockHeader setCompression(Compression.Algorithm compression) {
        this.compression = compression;
        return this;
    }

    @Override
    public int getUncompressedSize() {
        return uncompressedSize;
    }

    @Override
    public Log.BlockHeader setUncompressedSize(int uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
        return this;
    }

    @Override
    public int getCompressedSize() {
        return compressedSize;
    }

    @Override
    public Log.BlockHeader setCompressedSize(int compressedSize) {
        this.compressedSize = compressedSize;
        return this;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] magic = new byte[Log.BlockHeader.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(Log.BlockHeader.MAGIC, magic)) {
            throw new IOException("Invalid Log block magic. Got " + Bytes.toStringBinary(magic)
                + ", expected " + Bytes.toStringBinary(Log.BlockHeader.MAGIC));
        }
        version = in.readByte();
        if (version != Log.BlockHeader.VERSION) {
            throw new IOException("Unsupported Log block header version. Got " + version
                + ", expected " + Log.BlockHeader.VERSION);
        }
        int ordinal = in.readByte();
        compression = Compression.Algorithm.values()[ordinal];
        uncompressedSize = in.readInt();
        compressedSize = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(Log.BlockHeader.MAGIC);
        out.writeByte(version);
        out.writeByte((byte)compression.ordinal());
        out.writeInt(uncompressedSize);
        out.writeInt(compressedSize);
    }

    @Override
    public int getSerializedLength() {
        return HEADER_SIZE;
    }

    @Override
    public String toString() {
        return "LogBlockHeader [version=" + version + ", compressionAlgorithm="
            + compression + ", uncompressedSize=" + uncompressedSize + ", compressedSize="
            + compressedSize + "]";
    }

}
