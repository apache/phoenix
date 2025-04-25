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

public class LogBlockHeader implements LogFile.BlockHeader {
    private int version;
    private Compression.Algorithm compression;
    private int uncompressedSize;
    private int compressedSize;

    public static final int HEADER_SIZE = LogFile.BlockHeader.MAGIC.length
        + (2 * Bytes.SIZEOF_BYTE) + (2 * Bytes.SIZEOF_INT);

    public LogBlockHeader() {
        this.version = LogFile.BlockHeader.VERSION;
        this.compression = Compression.Algorithm.NONE;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public Compression.Algorithm getDataCompression() {
        return compression;
    }

    @Override
    public LogFile.BlockHeader setDataCompression(Compression.Algorithm compression) {
        this.compression = compression;
        return this;
    }

    @Override
    public int getUncompressedDataSize() {
        return uncompressedSize;
    }

    @Override
    public LogFile.BlockHeader setUncompressedDataSize(int uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
        return this;
    }

    @Override
    public int getCompressedDataSize() {
        return compressedSize;
    }

    @Override
    public LogFile.BlockHeader setCompressedDataSize(int compressedSize) {
        this.compressedSize = compressedSize;
        return this;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte[] magic = new byte[LogFile.BlockHeader.MAGIC.length];
        in.readFully(magic);
        if (!Arrays.equals(LogFile.BlockHeader.MAGIC, magic)) {
            throw new IOException("Invalid Log block magic. Got " + Bytes.toStringBinary(magic)
                + ", expected " + Bytes.toStringBinary(LogFile.BlockHeader.MAGIC));
        }
        version = in.readByte();
        if (version != LogFile.BlockHeader.VERSION) {
            throw new IOException("Unsupported Log block header version. Got " + version
                + ", expected " + LogFile.BlockHeader.VERSION);
        }
        int ordinal = in.readByte();
        compression = Compression.Algorithm.values()[ordinal];
        uncompressedSize = in.readInt();
        compressedSize = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(LogFile.BlockHeader.MAGIC);
        out.writeByte(version);
        out.writeByte((byte)compression.ordinal());
        out.writeInt(uncompressedSize);
        out.writeInt(compressedSize);
    }

    @Override
    public int getSerializedHeaderLength() {
        return HEADER_SIZE;
    }

    @Override
    public String toString() {
        return "LogBlockHeader [version=" + version + ", compressionAlgorithm="
            + compression + ", uncompressedSize=" + uncompressedSize + ", compressedSize="
            + compressedSize + "]";
    }

}
