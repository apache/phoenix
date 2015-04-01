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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.IndexTestingUtils;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Simple test to read/write simple files via our custom {@link WALCellCodec} to ensure properly
 * encoding/decoding without going through a cluster.
 */
public class ReadWriteKeyValuesWithCodecIT {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setupCodec() {
    Configuration conf = UTIL.getConfiguration();
    IndexTestingUtils.setupConfig(conf);
    conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
  }

  @Test
  public void testWithoutCompression() throws Exception {
    // get the FS ready to read/write the edits
    Path testDir = UTIL.getDataTestDir("TestReadWriteCustomEdits_withoutCompression");
    Path testFile = new Path(testDir, "testfile");
    FileSystem fs = UTIL.getTestFileSystem();

    List<WALEdit> edits = getEdits();
    writeReadAndVerify(null, fs, edits, testFile);
  }

  @Test
  public void testWithCompression() throws Exception {
    // get the FS ready to read/write the edit
    Path testDir = UTIL.getDataTestDir("TestReadWriteCustomEdits_withCompression");
    Path testFile = new Path(testDir, "testfile");
    FileSystem fs = UTIL.getTestFileSystem();

    List<WALEdit> edits = getEdits();
    CompressionContext compression = new CompressionContext(LRUDictionary.class, false, false);
    writeReadAndVerify(compression, fs, edits, testFile);
  }

  /**
   * @return a bunch of {@link WALEdit}s that test a range of serialization possibilities.
   */
  private List<WALEdit> getEdits() {
    // Build up a couple of edits
    List<WALEdit> edits = new ArrayList<WALEdit>();
    Put p = new Put(ROW);
    p.add(FAMILY, null, Bytes.toBytes("v1"));

    WALEdit withPut = new WALEdit();
    addMutation(withPut, p, FAMILY);
    edits.add(withPut);

    Delete d = new Delete(ROW);
    d.deleteColumn(FAMILY, null);
    WALEdit withDelete = new WALEdit();
    addMutation(withDelete, d, FAMILY);
    edits.add(withDelete);
    
    WALEdit withPutsAndDeletes = new WALEdit();
    addMutation(withPutsAndDeletes, d, FAMILY);
    addMutation(withPutsAndDeletes, p, FAMILY);
    edits.add(withPutsAndDeletes);
    
    WALEdit justIndexUpdates = new WALEdit();
    byte[] table = Bytes.toBytes("targetTable");
    IndexedKeyValue ikv = new IndexedKeyValue(table, p);
    justIndexUpdates.add(ikv);
    edits.add(justIndexUpdates);

    WALEdit mixed = new WALEdit();
    addMutation(mixed, d, FAMILY);
    mixed.add(ikv);
    addMutation(mixed, p, FAMILY);
    edits.add(mixed);

    return edits;
  }

  /**
   * Add all the {@link KeyValue}s in the {@link Mutation}, for the pass family, to the given
   * {@link WALEdit}.
   */
  private void addMutation(WALEdit edit, Mutation m, byte[] family) {
    List<Cell> kvs = m.getFamilyCellMap().get(FAMILY);
    for (Cell kv : kvs) {
      edit.add(KeyValueUtil.ensureKeyValue(kv));
    }
  }

  
  private void writeWALEdit(WALCellCodec codec, List<Cell> kvs, FSDataOutputStream out) throws IOException {
    out.writeInt(kvs.size());
    Codec.Encoder cellEncoder = codec.getEncoder(out);
    // We interleave the two lists for code simplicity
    for (Cell kv : kvs) {
        cellEncoder.write(kv);
    }
  }
  
  /**
   * Write the edits to the specified path on the {@link FileSystem} using the given codec and then
   * read them back in and ensure that we read the same thing we wrote.
   */
  private void writeReadAndVerify(final CompressionContext compressionContext, FileSystem fs, List<WALEdit> edits,
      Path testFile) throws IOException {
	  
	WALCellCodec codec = WALCellCodec.create(UTIL.getConfiguration(), compressionContext);  
    // write the edits out
    FSDataOutputStream out = fs.create(testFile);
    for (WALEdit edit : edits) {
      writeWALEdit(codec, edit.getCells(), out);
    }
    out.close();

    // read in the edits
    FSDataInputStream in = fs.open(testFile);
    List<WALEdit> read = new ArrayList<WALEdit>();
    for (int i = 0; i < edits.size(); i++) {
      WALEdit edit = new WALEdit();
      int numEdits = in.readInt();
      edit.readFromCells(codec.getDecoder(in), numEdits);
      read.add(edit);
    }
    in.close();

    // make sure the read edits match the written
    for(int i=0; i< edits.size(); i++){
      WALEdit expected = edits.get(i);
      WALEdit found = read.get(i);
      for(int j=0; j< expected.getCells().size(); j++){
        Cell fkv = found.getCells().get(j);
        Cell ekv = expected.getCells().get(j);
        assertEquals("KV mismatch for edit! Expected: "+expected+", but found: "+found, ekv, fkv);
      }
    }
  }
}
