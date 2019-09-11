/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a copy paste version of org.apache.hadoop.hbase.codec.BaseDecoder class. 
 * This class is meant to be used in {@link IndexedWALEditCodec} when runtime version of
 * HBase is older than 1.1.3. This is needed to handle binary incompatibility introduced by
 * HBASE-14501. See PHOENIX-2629 and PHOENIX-2636 for details.
 */
public abstract class BinaryCompatibleBaseDecoder implements Codec.Decoder {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BinaryCompatibleBaseDecoder.class);

  protected final InputStream in;
  private Cell current = null;

  protected static class PBIS extends PushbackInputStream {
    public PBIS(InputStream in, int size) {
      super(in, size);
    }

    public void resetBuf(int size) {
      this.buf = new byte[size];
      this.pos = size;
    }
  }

  public BinaryCompatibleBaseDecoder(final InputStream in) {
    this.in = new PBIS(in, 1);
  }

  @Override
  public boolean advance() throws IOException {
    int firstByte = in.read();
    if (firstByte == -1) {
      return false;
    } else {
      ((PBIS)in).unread(firstByte);
    }

    try {
      this.current = parseCell();
    } catch (IOException ioEx) {
      ((PBIS)in).resetBuf(1); // reset the buffer in case the underlying stream is read from upper layers
      rethrowEofException(ioEx);
    }
    return true;
  }

  private void rethrowEofException(IOException ioEx) throws IOException {
    boolean isEof = false;
    try {
      isEof = this.in.available() == 0;
    } catch (Throwable t) {
      LOGGER.trace("Error getting available for error message - ignoring", t);
    }
    if (!isEof) throw ioEx;
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Partial cell read caused by EOF", ioEx);
    }
    EOFException eofEx = new EOFException("Partial cell read");
    eofEx.initCause(ioEx);
    throw eofEx;
  }

  protected InputStream getInputStream() {
    return in;
  }

  /**
   * Extract a Cell.
   * @return a parsed Cell or throws an Exception. EOFException or a generic IOException maybe
   * thrown if EOF is reached prematurely. Does not return null.
   * @throws IOException
   */
  @Nonnull
  protected abstract Cell parseCell() throws IOException;

  @Override
  public Cell current() {
    return this.current;
  }
}