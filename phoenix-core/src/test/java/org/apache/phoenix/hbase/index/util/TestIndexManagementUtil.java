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
package org.apache.phoenix.hbase.index.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.wal.IndexedHLogReader;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.phoenix.hbase.index.covered.CoveredColumnIndexCodec;
import org.junit.Test;

public class TestIndexManagementUtil {

  @Test
  public void testUncompressedWal() throws Exception {
    Configuration conf = new Configuration(false);
    // works with WALEditcodec
    conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
    // clear the codec and set the wal reader
    conf = new Configuration(false);
    conf.set(IndexManagementUtil.HLOG_READER_IMPL_KEY, IndexedHLogReader.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
  }

  /**
   * Compressed WALs are supported when we have the WALEditCodec installed
   * @throws Exception
   */
  @Test
  public void testCompressedWALWithCodec() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    // works with WALEditcodec
    conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
  }

  /**
   * We cannot support WAL Compression with the IndexedHLogReader
   * @throws Exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCompressedWALWithHLogReader() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    // works with WALEditcodec
    conf.set(IndexManagementUtil.HLOG_READER_IMPL_KEY, IndexedHLogReader.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
  }

  /**
   * Create the specified index table with the necessary columns
   * @param admin {@link Admin} to use when creating the table
   * @param indexTable name of the index table.
   * @throws IOException
   */
  public static void createIndexTable(Admin admin, String indexTable) throws IOException {
    createIndexTable(admin, TableDescriptorBuilder.newBuilder(TableName.valueOf(indexTable)));
  }

  /**
   * @param admin to create the table
   * @param index descriptor to update before creating table
   */
  public static void createIndexTable(Admin admin, TableDescriptorBuilder indexBuilder) throws IOException {
        indexBuilder.addColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(CoveredColumnIndexCodec.INDEX_ROW_COLUMN_FAMILY)
                        .setKeepDeletedCells(KeepDeletedCells.TRUE).build());
    admin.createTable(indexBuilder.build());
  }
}