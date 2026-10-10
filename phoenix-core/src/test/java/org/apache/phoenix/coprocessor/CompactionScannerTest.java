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
package org.apache.phoenix.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

public class CompactionScannerTest {

  private static final TableName TABLE_NAME = TableName.valueOf("T");

  private static TableDescriptor descriptor(String... families) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
    for (String family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(family)));
    }
    return builder.build();
  }

  // Builds the scanner of a minor compaction. A major compaction needs a server connection to
  // resolve the TTL. The constructor makes the same store decisions for both types.
  private static boolean isRegionLevel(TableDescriptor descriptor, String family)
    throws IOException {
    Region region = mock(Region.class);
    when(region.getTableDescriptor()).thenReturn(descriptor);
    when(region.getRegionInfo()).thenReturn(RegionInfoBuilder.newBuilder(TABLE_NAME).build());
    RegionCoprocessorEnvironment env = mock(RegionCoprocessorEnvironment.class);
    when(env.getRegion()).thenReturn(region);
    when(env.getConfiguration()).thenReturn(new Configuration(false));
    Store store = mock(Store.class);
    when(store.getColumnFamilyName()).thenReturn(family);
    when(store.getColumnFamilyDescriptor())
      .thenReturn(descriptor.getColumnFamily(Bytes.toBytes(family)));
    PTable table = mock(PTable.class);
    when(table.getName()).thenReturn(PNameFactory.newName("T"));
    when(table.getTableName()).thenReturn(PNameFactory.newName("T"));
    when(table.getEncodingScheme())
      .thenReturn(PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS);
    CompactionScanner scanner =
      new CompactionScanner(env, store, mock(InternalScanner.class), 0, false, false, table);
    return scanner.isRegionLevelStore();
  }

  @Test
  public void testVectorFamilyKeepsSingleFamilyStoreLevelCompaction() throws IOException {
    String emptyCF = QueryConstants.DEFAULT_COLUMN_FAMILY;
    TableDescriptor plain = descriptor(emptyCF);
    TableDescriptor withVector =
      descriptor(emptyCF, QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY);
    assertEquals(1, CompactionScanner.getDataFamilyCount(plain));
    assertEquals(1, CompactionScanner.getDataFamilyCount(withVector));
    assertFalse(isRegionLevel(plain, emptyCF));
    assertFalse(isRegionLevel(withVector, emptyCF));
  }

  @Test
  public void testVectorFamilyKeepsRegionLevelCompactionForDataFamilies() throws IOException {
    String emptyCF = QueryConstants.DEFAULT_COLUMN_FAMILY;
    TableDescriptor withVector =
      descriptor(emptyCF, "A", QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY);
    assertEquals(2, CompactionScanner.getDataFamilyCount(withVector));
    assertTrue(isRegionLevel(withVector, emptyCF));
    // The other data family compacts at the store level. It uses the region level only for rows
    // that it cannot decide alone.
    assertFalse(isRegionLevel(withVector, "A"));
    // A minor compaction never compacts every row at the region level.
    assertFalse(CompactionScanner.isRegionLevelCompaction(false, 2, false, true));
  }

  @Test
  public void testLocalIndexFamiliesKeepTheirHandling() throws IOException {
    String emptyCF = QueryConstants.DEFAULT_COLUMN_FAMILY;
    String localIndexCF = QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX + emptyCF;
    TableDescriptor withVector =
      descriptor(emptyCF, localIndexCF, QueryConstants.VECTOR_LOCAL_INDEX_COLUMN_FAMILY);
    assertEquals(2, CompactionScanner.getDataFamilyCount(withVector));
    assertTrue(isRegionLevel(withVector, emptyCF));
    assertFalse(isRegionLevel(withVector, localIndexCF));
  }
}
