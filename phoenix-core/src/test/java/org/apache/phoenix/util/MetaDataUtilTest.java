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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.client.ClientKeyValueBuilder;
import org.apache.phoenix.client.GenericKeyValueBuilder;
import org.apache.phoenix.client.KeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.junit.Test;


public class MetaDataUtilTest {

    @Test
    public void testEncode() {
        assertEquals(MetaDataUtil.encodeVersion("0.94.5"),MetaDataUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.6")>MetaDataUtil.encodeVersion("0.94.5-mapR"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.6")>MetaDataUtil.encodeVersion("0.94.5"));
        assertTrue(MetaDataUtil.encodeVersion("0.94.1-mapR")>MetaDataUtil.encodeVersion("0.94"));
    }
    
    @Test
    public void testCompatibility() {
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,1), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,10), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,0), 1, 2));
        assertTrue(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(1,2,255), 1, 2));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(2,2,0), 2, 1));
        assertFalse(MetaDataUtil.areClientAndServerCompatible(MetaDataUtil.encodeVersion(3,1,10), 4, 2));
    }

  /**
   * Ensure it supports both the {@link GenericKeyValueBuilder} and {@link ClientKeyValueBuilder}
   * @throws Exception on failure
   */
  @Test
  public void testGetMutationKeyValue() throws Exception {
    KeyValueBuilder builder = GenericKeyValueBuilder.INSTANCE;
    byte[] row = Bytes.toBytes("row");
    byte[] family = PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] value = Bytes.toBytes("generic-value");
    KeyValue kv = builder.buildPut(wrap(row), wrap(family), wrap(qualifier), wrap(value));
    Put put = new Put(row);
    KeyValueBuilder.addQuietly(put, builder, kv);

    // read back out the value
    ImmutableBytesPtr ptr = new ImmutableBytesPtr();
    assertTrue(MetaDataUtil.getMutationKeyValue(put, qualifier, builder, ptr));
    assertEquals("Value returned doesn't match stored value for " + builder.getClass().getName()
        + "!", 0,
      ByteUtil.BYTES_PTR_COMPARATOR.compare(ptr, wrap(value)));

    // try again, this time with the clientkeyvalue builder
    builder = ClientKeyValueBuilder.INSTANCE;
    value = Bytes.toBytes("client-value");
    kv = builder.buildPut(wrap(row), wrap(family), wrap(qualifier), wrap(value));
    put = new Put(row);
    KeyValueBuilder.addQuietly(put, builder, kv);

    // read back out the value
    assertTrue(MetaDataUtil.getMutationKeyValue(put, qualifier, builder, ptr));
    assertEquals("Value returned doesn't match stored value for " + builder.getClass().getName()
        + "!", 0,
      ByteUtil.BYTES_PTR_COMPARATOR.compare(ptr, wrap(value)));

    // ensure that we don't get matches for qualifiers that don't match
    assertFalse(MetaDataUtil.getMutationKeyValue(put, Bytes.toBytes("not a match"), builder, ptr));
  }

  private static ImmutableBytesPtr wrap(byte[] bytes) {
    return new ImmutableBytesPtr(bytes);
  }
}

