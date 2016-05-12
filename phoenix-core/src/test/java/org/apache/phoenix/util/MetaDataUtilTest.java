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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;


public class MetaDataUtilTest {

    @Test
    public void testEncode() {
        assertEquals(VersionUtil.encodeVersion("0.94.5"),VersionUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(VersionUtil.encodeVersion("0.94.6")>VersionUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(VersionUtil.encodeVersion("0.94.6")>VersionUtil.encodeVersion("0.94.5"));
        assertTrue(VersionUtil.encodeVersion("0.94.1-mapR")>VersionUtil.encodeVersion("0.94"));
        assertTrue(VersionUtil.encodeVersion("1", "1", "3")>VersionUtil.encodeVersion("1", "1", "1"));
    }
    
    @Test
    public void testCompatibility() {
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,1), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,10), 1, 1));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,0), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(1,2,255), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(2,2,0), 2, 0));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(2,10,36), 2, 9));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 4, 0));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 2, 0));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 3, 2));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(VersionUtil.encodeVersion(3,1,10), 3, 5));
    }

  /**
   * Ensure it supports {@link GenericKeyValueBuilder}
   * @throws Exception on failure
   */
  @Test
  public void testGetMutationKeyValue() throws Exception {
    String version = VersionInfo.getVersion();
    KeyValueBuilder builder = KeyValueBuilder.get(version);
    byte[] row = Bytes.toBytes("row");
    byte[] family = PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] value = Bytes.toBytes("generic-value");
    KeyValue kv = builder.buildPut(wrap(row), wrap(family), wrap(qualifier), wrap(value));
    Put put = new Put(row);
    KeyValueBuilder.addQuietly(put, builder, kv);

    // read back out the value
    ImmutableBytesPtr ptr = new ImmutableBytesPtr();
    assertTrue(MetaDataUtil.getMutationValue(put, qualifier, builder, ptr));
    assertEquals("Value returned doesn't match stored value for " + builder.getClass().getName()
        + "!", 0,
      ByteUtil.BYTES_PTR_COMPARATOR.compare(ptr, wrap(value)));

    // try again, this time with the clientkeyvalue builder
    if (builder != GenericKeyValueBuilder.INSTANCE) {
        builder = GenericKeyValueBuilder.INSTANCE;
        value = Bytes.toBytes("client-value");
        kv = builder.buildPut(wrap(row), wrap(family), wrap(qualifier), wrap(value));
        put = new Put(row);
        KeyValueBuilder.addQuietly(put, builder, kv);
    
        // read back out the value
        assertTrue(MetaDataUtil.getMutationValue(put, qualifier, builder, ptr));
        assertEquals("Value returned doesn't match stored value for " + builder.getClass().getName()
            + "!", 0,
          ByteUtil.BYTES_PTR_COMPARATOR.compare(ptr, wrap(value)));
    
        // ensure that we don't get matches for qualifiers that don't match
        assertFalse(MetaDataUtil.getMutationValue(put, Bytes.toBytes("not a match"), builder, ptr));
    }
  }

  private static ImmutableBytesPtr wrap(byte[] bytes) {
    return new ImmutableBytesPtr(bytes);
  }

    @Test
    public void testEncodeDecode() {
        String hbaseVersionStr = "0.98.14";
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.setBoolean(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, false);
        config.setBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, false);

        long version = MetaDataUtil.encodeVersion(hbaseVersionStr, config);
        int hbaseVersion = MetaDataUtil.decodeHBaseVersion(version);
        int expectedHBaseVersion = VersionUtil.encodeVersion(0, 98, 14);
        assertEquals(expectedHBaseVersion, hbaseVersion);
        boolean isTableNamespaceMappingEnabled = MetaDataUtil.decodeTableNamespaceMappingEnabled(version);
        assertFalse(isTableNamespaceMappingEnabled);
        int phoenixVersion = MetaDataUtil.decodePhoenixVersion(version);
        int expectedPhoenixVersion = VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION,
                MetaDataProtocol.PHOENIX_MINOR_VERSION, MetaDataProtocol.PHOENIX_PATCH_NUMBER);
        assertEquals(expectedPhoenixVersion, phoenixVersion);

        config.setBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, true);

        version = MetaDataUtil.encodeVersion(hbaseVersionStr, config);
        hbaseVersion = MetaDataUtil.decodeHBaseVersion(version);
        expectedHBaseVersion = VersionUtil.encodeVersion(0, 98, 14);
        assertEquals(expectedHBaseVersion, hbaseVersion);
        isTableNamespaceMappingEnabled = MetaDataUtil.decodeTableNamespaceMappingEnabled(version);
        assertTrue(isTableNamespaceMappingEnabled);
        phoenixVersion = MetaDataUtil.decodePhoenixVersion(version);
        expectedPhoenixVersion = VersionUtil.encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION,
                MetaDataProtocol.PHOENIX_MINOR_VERSION, MetaDataProtocol.PHOENIX_PATCH_NUMBER);
        assertEquals(expectedPhoenixVersion, phoenixVersion);
    }
}

