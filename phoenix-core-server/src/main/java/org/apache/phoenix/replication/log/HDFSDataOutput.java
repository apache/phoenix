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
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * SyncableDataOutput implementation that delegates to a standard FSDataOutputStream.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class HDFSDataOutput implements SyncableDataOutput {

  private final FSDataOutputStream delegate;

  public HDFSDataOutput(FSDataOutputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);
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
  public void writeBoolean(boolean v) throws IOException {
    delegate.writeBoolean(v);
  }

  @Override
  public void writeByte(int v) throws IOException {
    delegate.writeByte(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    delegate.writeShort(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    delegate.writeChar(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    delegate.writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    delegate.writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    delegate.writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    delegate.writeDouble(v);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    delegate.writeBytes(s);
  }

  @Override
  public void writeChars(String s) throws IOException {
    delegate.writeChars(s);
  }

  @Override
  public void writeUTF(String s) throws IOException {
    delegate.writeUTF(s);
  }

  @Override
  public void hflush() throws IOException {
    delegate.hflush();
  }

  @Override
  public void hsync() throws IOException {
    delegate.hsync();
  }

  @Override
  public void sync() throws IOException {
    delegate.hsync();
  }

}
