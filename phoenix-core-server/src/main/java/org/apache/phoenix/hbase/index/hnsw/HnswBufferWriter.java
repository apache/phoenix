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
package org.apache.phoenix.hbase.index.hnsw;

import io.github.jbellis.jvector.disk.IndexWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Direct memory {@link IndexWriter} implementation supporting in-memory serialization of JVector
 * graph segments and key trailers. Direct byte buffers expand dynamically up to safe allocation
 * limits.
 * <p>
 * Instances are not thread-safe.
 */
public final class HnswBufferWriter implements IndexWriter {
  // Maximum safe byte buffer capacity
  private static final int MAX_CAPACITY = 1024 * 1024 * 1024; // 1 GB

  private ByteBuffer buffer;

  /** Initializes a direct buffer writer with the specified initial byte capacity. */
  public HnswBufferWriter(int capacity) {
    buffer = ByteBuffer.allocateDirect(Math.max(capacity, 16)).order(ByteOrder.BIG_ENDIAN);
  }

  // Expands the direct buffer if remaining capacity cannot accommodate the incoming write
  private void ensure(int bytes) throws IOException {
    if (buffer.remaining() >= bytes) {
      return;
    }
    long needed = (long) buffer.position() + bytes;
    if (needed > MAX_CAPACITY) {
      throw new IOException("HNSW segment exceeds " + MAX_CAPACITY + " bytes");
    }
    int capacity = (int) Math.min(MAX_CAPACITY, Math.max(needed, 2L * buffer.capacity()));
    ByteBuffer grown = ByteBuffer.allocateDirect(capacity).order(ByteOrder.BIG_ENDIAN);
    buffer.flip();
    grown.put(buffer);
    buffer = grown;
  }

  /** Current buffer capacity in bytes. */
  public int capacity() {
    return buffer.capacity();
  }

  /** Copies written buffer contents up to the current write position. */
  public byte[] toByteArray() {
    byte[] bytes = new byte[buffer.position()];
    ByteBuffer written = buffer.duplicate();
    written.flip();
    written.get(bytes);
    return bytes;
  }

  @Override
  public long position() {
    return buffer.position();
  }

  @Override
  public void write(int b) throws IOException {
    ensure(1);
    buffer.put((byte) b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensure(len);
    buffer.put(b, off, len);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  @Override
  public void writeByte(int v) throws IOException {
    write(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    ensure(Short.BYTES);
    buffer.putShort((short) v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    ensure(Character.BYTES);
    buffer.putChar((char) v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    ensure(Integer.BYTES);
    buffer.putInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    ensure(Long.BYTES);
    buffer.putLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    ensure(Float.BYTES);
    buffer.putFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    ensure(Double.BYTES);
    buffer.putDouble(v);
  }

  @Override
  public void writeFloats(float[] floats, int offset, int count) throws IOException {
    ensure(count * Float.BYTES);
    buffer.asFloatBuffer().put(floats, offset, count);
    buffer.position(buffer.position() + count * Float.BYTES);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    ensure(s.length());
    for (int i = 0; i < s.length(); i++) {
      buffer.put((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChars(String s) throws IOException {
    ensure(s.length() * Character.BYTES);
    for (int i = 0; i < s.length(); i++) {
      buffer.putChar(s.charAt(i));
    }
  }

  @Override
  public void writeUTF(String s) throws IOException {
    // Delegate to standard DataOutputStream for Java modified UTF-8 encoding
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(s.length() + 2);
    new DataOutputStream(bytes).writeUTF(s);
    write(bytes.toByteArray());
  }

  @Override
  public void close() {
    // Direct memory is reclaimed on garbage collection when references are cleared
  }
}
