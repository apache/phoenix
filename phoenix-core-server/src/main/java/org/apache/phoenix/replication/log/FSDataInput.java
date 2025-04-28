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

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Input side abstraction, mirroring the write side FSDataOutput.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2",
    justification="Intentional")
public class FSDataInput implements DataInput, Closeable, SeekableDataInput {

    private final FSDataInputStream delegate;

    public FSDataInput(FSDataInputStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        delegate.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        delegate.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return delegate.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return delegate.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return delegate.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return delegate.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return delegate.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return delegate.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return delegate.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return delegate.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return delegate.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return delegate.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return delegate.readDouble();
    }

    @Override
    @Deprecated
    public String readLine() throws IOException {
        return delegate.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return delegate.readUTF();
    }

    @Override
    public void seek(long pos) throws IOException {
        delegate.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return delegate.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return delegate.seekToNewSource(targetPos);
    }

    public int read(byte[] buffer) throws IOException {
        return delegate.read(buffer);
    }

    public int read(byte[] buffer, int pos, int len) throws IOException {
        return delegate.read(buffer, pos, len);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return delegate.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        delegate.readFully(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        delegate.readFully(position, buffer);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

}
