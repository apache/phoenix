/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.codec.BaseDecoder;
import org.apache.hadoop.hbase.codec.BaseEncoder;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.apache.phoenix.hbase.index.wal.KeyValueCodec;


/**
 * Support custom indexing {@link KeyValue}s when written to the WAL.
 * <p>
 * Currently, we don't support reading older WAL files - only new WAL files. Therefore, this should
 * not be installed on a running cluster, but rather one that has been cleanly shutdown and requires
 * no WAL replay on startup.
 */
public class IndexedWALEditCodec extends WALCellCodec {

  // can't have negative values because reading off a stream returns a negative if its the end of
  // the stream
  private static final int REGULAR_KEY_VALUE_MARKER = 0;
  private CompressionContext compression;
  private static final int MIN_BINARY_COMPATIBLE_INDEX_CODEC_VERSION = VersionUtil.encodeVersion("1", "1", "3");
  private final boolean useDefaultDecoder;

  private static boolean isUseDefaultDecoder() {
      String hbaseVersion = VersionInfo.getVersion();
      return VersionUtil.encodeVersion(hbaseVersion) >= MIN_BINARY_COMPATIBLE_INDEX_CODEC_VERSION;
  }

  /*
   * No-args constructor must be provided for WALSplitter/RPC Codec path
   */
  public IndexedWALEditCodec() {
      super();
      this.compression = null;
      this.useDefaultDecoder = isUseDefaultDecoder();
  }

  /*
   * Two-args Configuration and CompressionContext codec must be provided for WALCellCodec path
   */
  public IndexedWALEditCodec(Configuration conf, CompressionContext compression) {
      super(conf, compression);
      this.compression = compression;
      this.useDefaultDecoder = isUseDefaultDecoder();
  }

  @Override
  public Decoder getDecoder(InputStream is) {
    // compression isn't enabled
    if (this.compression == null) {
      return useDefaultDecoder ? new IndexKeyValueDecoder(is) : new BinaryCompatibleIndexKeyValueDecoder(is);
    }

    // there is compression, so we get the standard decoder to handle reading those kvs
    Decoder decoder = super.getDecoder(is);
    // compression is on, reqturn our custom decoder
    return useDefaultDecoder ? new CompressedIndexKeyValueDecoder(is, decoder) : new BinaryCompatibleCompressedIndexKeyValueDecoder(is, decoder);
  }

  @Override
  public Encoder getEncoder(OutputStream os) {
    // compression isn't on, do the default thing
    if (this.compression == null) {
      return new IndexKeyValueEncoder(os);
    }

    // compression is on, return our one that will handle putting in the correct markers
    Encoder encoder = super.getEncoder(os);
    return new CompressedIndexKeyValueEncoder(os, encoder);
  }

  /**
   * Returns a DataInput given an InputStream
   */
  private static DataInput getDataInput(InputStream is) {
    return is instanceof DataInput
        ? (DataInput) is
        : new DataInputStream(is);
  }

  /**
   * Returns a DataOutput given an OutputStream
   */
  private static DataOutput getDataOutput(OutputStream os) {
    return os instanceof DataOutput
        ? (DataOutput) os
        : new DataOutputStream(os);
  }

  private static abstract class PhoenixBaseDecoder extends BaseDecoder {
    protected DataInput dataInput;
    public PhoenixBaseDecoder(InputStream in) {
      super(in);
      dataInput = getDataInput(this.in);
    }
  }

  /**
   * Custom Decoder that can handle a stream of regular and indexed {@link KeyValue}s.
   */
  public static class IndexKeyValueDecoder extends PhoenixBaseDecoder {

    /**
     * Create a Decoder on the given input stream with the given Decoder to parse
     * generic {@link KeyValue}s.
     * @param is stream to read from
     */
    public IndexKeyValueDecoder(InputStream is){
      super(is);
    }

    @Override
    protected KeyValue parseCell() throws IOException{
      return KeyValueCodec.readKeyValue(this.dataInput);
    }
  }

  public static class CompressedIndexKeyValueDecoder extends PhoenixBaseDecoder {

    private Decoder decoder;

    /**
     * Create a Decoder on the given input stream with the given Decoder to parse
     * generic {@link KeyValue}s.
     * @param is stream to read from
     * @param compressedDecoder decoder for generic {@link KeyValue}s. Should support the expected
     *          compression.
     */
    public CompressedIndexKeyValueDecoder(InputStream is, Decoder compressedDecoder) {
      super(is);
      this.decoder = compressedDecoder;
    }

    @Override
    protected Cell parseCell() throws IOException {
      // reader the marker
      int marker = this.in.read();
      if (marker < 0) {
        throw new EOFException(
            "Unexepcted end of stream found while reading next (Indexed) KeyValue");
      }

      // do the normal thing, if its a regular kv
      if (marker == REGULAR_KEY_VALUE_MARKER) {
        if (!this.decoder.advance()) {
          throw new IOException("Could not read next key-value from generic KeyValue Decoder!");
        }
        return this.decoder.current();
      }

      // its an indexedKeyValue, so parse it out specially
      return KeyValueCodec.readKeyValue(this.dataInput);
    }
  }

  private static abstract class PhoenixBaseEncoder extends BaseEncoder {
    protected DataOutput dataOutput;
    public PhoenixBaseEncoder(OutputStream out) {
      super(out);
      dataOutput = getDataOutput(this.out);
    }
  }

  /**
   * Encode {@link IndexedKeyValue}s via the {@link KeyValueCodec}. Does <b>not</b> support
   * compression.
   */
  private static class IndexKeyValueEncoder extends PhoenixBaseEncoder {
    public IndexKeyValueEncoder(OutputStream os) {
      super(os);
    }

    @Override
    public void flush() throws IOException {
      super.flush();
    }

    @Override
    public void write(Cell cell) throws IOException {
      // make sure we are open
      checkFlushed();

      // use the standard encoding mechanism
      KeyValueCodec.write(this.dataOutput, KeyValueUtil.ensureKeyValue(cell));
    }
  }

  /**
   * Write {@link IndexedKeyValue}s along side compressed {@link KeyValue}s. This Encoder is
   * <b>not</b> compatible with the {@link IndexKeyValueDecoder} - one cannot intermingle compressed
   * and uncompressed WALs that contain index entries.
   */
  private static class CompressedIndexKeyValueEncoder extends PhoenixBaseEncoder {
    private Encoder compressedKvEncoder;

    public CompressedIndexKeyValueEncoder(OutputStream os, Encoder compressedKvEncoder) {
      super(os);
      this.compressedKvEncoder = compressedKvEncoder;
    }

    @Override
    public void flush() throws IOException {
      this.compressedKvEncoder.flush();
      super.flush();
    }

    @Override
    public void write(Cell cell) throws IOException {
      //make sure we are open
      checkFlushed();

      //write the special marker so we can figure out which kind of kv is it
      int marker = IndexedWALEditCodec.REGULAR_KEY_VALUE_MARKER;
      if (cell instanceof IndexedKeyValue) {
        marker = KeyValueCodec.INDEX_TYPE_LENGTH_MARKER;
      }
      out.write(marker);

      //then serialize based on the marker
      if (marker == IndexedWALEditCodec.REGULAR_KEY_VALUE_MARKER) {
        this.compressedKvEncoder.write(cell);
      }
      else{
        KeyValueCodec.write(this.dataOutput, KeyValueUtil.ensureKeyValue(cell));
      }
    }
  }
  
  private static abstract class BinaryCompatiblePhoenixBaseDecoder extends BinaryCompatibleBaseDecoder {
      protected DataInput dataInput;
      public BinaryCompatiblePhoenixBaseDecoder(InputStream in) {
        super(in);
        dataInput = getDataInput(this.in);
      } 
  }
  
  /**
   * This class is meant to be used when runtime version of HBase
   * HBase is older than 1.1.3. This is needed to handle binary incompatibility introduced by
   * HBASE-14501. See PHOENIX-2629 and PHOENIX-2636 for details.
   */
  private static class BinaryCompatibleIndexKeyValueDecoder extends BinaryCompatiblePhoenixBaseDecoder {
      /**
       * Create a Decoder on the given input stream with the given Decoder to parse
       * generic {@link KeyValue}s.
       * @param is stream to read from
       */
      public BinaryCompatibleIndexKeyValueDecoder(InputStream is){
        super(is);
      }

      @Override
      protected KeyValue parseCell() throws IOException{
        return KeyValueCodec.readKeyValue(this.dataInput);
      }
  }
  
  /**
   * This class is meant to be used when runtime version of HBase
   * HBase is older than 1.1.3. This is needed to handle binary incompatibility introduced by
   * HBASE-14501. See PHOENIX-2629 and PHOENIX-2636 for details.
   */
  private static class BinaryCompatibleCompressedIndexKeyValueDecoder extends BinaryCompatiblePhoenixBaseDecoder {

      private Decoder decoder;

      /**
       * Create a Decoder on the given input stream with the given Decoder to parse
       * generic {@link KeyValue}s.
       * @param is stream to read from
       * @param compressedDecoder decoder for generic {@link KeyValue}s. Should support the expected
       *          compression.
       */
      public BinaryCompatibleCompressedIndexKeyValueDecoder(InputStream is, Decoder compressedDecoder) {
        super(is);
        this.decoder = compressedDecoder;
      }

      @Override
      protected Cell parseCell() throws IOException {
        // reader the marker
        int marker = this.in.read();
        if (marker < 0) {
          throw new EOFException(
              "Unexepcted end of stream found while reading next (Indexed) KeyValue");
        }

        // do the normal thing, if its a regular kv
        if (marker == REGULAR_KEY_VALUE_MARKER) {
          if (!this.decoder.advance()) {
            throw new IOException("Could not read next key-value from generic KeyValue Decoder!");
          }
          return this.decoder.current();
        }

        // its an indexedKeyValue, so parse it out specially
        return KeyValueCodec.readKeyValue(this.dataInput);
      }
  }
  
}