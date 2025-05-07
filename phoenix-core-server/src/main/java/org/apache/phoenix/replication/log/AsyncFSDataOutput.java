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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.io.asyncfs.AsyncFSOutput;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * SyncableDataOutput implementation that delegates to a hbase-async AsyncFSOutput.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class AsyncFSDataOutput implements SyncableDataOutput {

    private final AsyncFSOutput delegate;

    public AsyncFSDataOutput(AsyncFSOutput delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
        return delegate.getSyncedLength() + delegate.buffered();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private byte[] byteBuf = new byte[1];

    @Override
    public void write(int b) throws IOException {
        byteBuf[0] = (byte) b;
        delegate.write(byteBuf);
    }

    @Override
    public void write(byte[] b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void writeByte(int v) throws IOException {
        byteBuf[0] = (byte) v;
        delegate.write(byteBuf);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0);
    }

    @Override
    public void writeShort(int v) throws IOException {
        // This code is equivalent to what DataOutputStream in the JRE does.
        writeByte((v >>> 8) & 0xFF);
        writeByte((v >>> 0) & 0xFF);
    }

    @Override
    public void writeChar(int v) throws IOException {
        // What DataOutputStream in the JRE does is equivalent to writeShort.
        writeShort(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        delegate.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        delegate.write(Bytes.toBytes(v));
    }

    @Override
    public void writeFloat(float v) throws IOException {
        delegate.write(Bytes.toBytes(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        delegate.write(Bytes.toBytes(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        writeUTF(s); // Simplify here by unconditionally coding strings as UTF-8.
    }

    @Override
    public void writeChars(String s) throws IOException {
        // DataOutputStream in the JRE does the equivalent of writeShort on each code point.
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            writeShort(v);
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        delegate.write(Bytes.toBytes(s));
    }

    @Override
    public void hflush() throws IOException {
        try {
            // Our sync method is synchronous, as intended.
            delegate.flush(false).get();
        } catch (InterruptedException e) {
            InterruptedIOException ioe = new InterruptedIOException();
            ioe.initCause(e);
            throw ioe;
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void hsync() throws IOException {
        try {
            // Our sync method is synchronous, as intended.
            delegate.flush(true).get();
        } catch (InterruptedException e) {
            InterruptedIOException ioe = new InterruptedIOException();
            ioe.initCause(e);
            throw ioe;
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void sync() throws IOException {
        hsync();
    }

}
