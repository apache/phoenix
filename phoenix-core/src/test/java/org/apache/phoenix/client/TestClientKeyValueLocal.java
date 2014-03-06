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
package org.apache.phoenix.client;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TestKeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.phoenix.hbase.index.util.ClientKeyValue;
import org.junit.Test;

/**
 * Ensure that we can accss a {@link ClientKeyValue} as expected. For instance, write it to bytes
 * and then read it back into a {@link KeyValue} as expected or compare columns to other
 * {@link KeyValue}s.
 */
public class TestClientKeyValueLocal {

  @Test
  public void testReadWrite() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    long ts = 10;
    Type type = KeyValue.Type.Put;
    ClientKeyValue kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type,
        wrap(value));
    validate(kv, row, family, qualifier, ts, type, value);

    type = Type.Delete;
    kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type, wrap(value));
    validate(kv, row, family, qualifier, ts, type, value);

    type = Type.DeleteColumn;
    kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type, wrap(value));
    validate(kv, row, family, qualifier, ts, type, value);

    type = Type.DeleteFamily;
    kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type, wrap(value));
    validate(kv, row, family, qualifier, ts, type, value);

    type = Type.Maximum;
    kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type, wrap(value));
    validate(kv, row, family, qualifier, ts, type, value);

    // test a couple different variables, to make sure we aren't faking it
    row = Bytes.toBytes("row-never-seen-before1234");
    family = Bytes.toBytes("family-to-test-more");
    qualifier = Bytes.toBytes("untested-qualifier");
    value = Bytes.toBytes("value-that-we-haven't_tested");
    ts = System.currentTimeMillis();
    kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type, wrap(value));
    validate(kv, row, family, qualifier, ts, type, value);
  }

  /**
   * Corner case where values can be null
   * @throws IOException
   */
  @Test
  public void testNullValues() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] family = Bytes.toBytes("family");
    byte[] qualifier = Bytes.toBytes("qualifier");
    long ts = 10;
    Type type = KeyValue.Type.Put;
    byte[] empty = new byte[0];
    // values can be null
    ClientKeyValue kv = new ClientKeyValue(wrap(row), wrap(family), wrap(qualifier), ts, type, null);
    validate(kv, row, family, qualifier, ts, type, empty);
    kv = new ClientKeyValue(row, family, qualifier, ts, type, null);
    validate(kv, row, family, qualifier, ts, type, empty);
    kv = new ClientKeyValue(row, family, qualifier, ts, type);
    validate(kv, row, family, qualifier, ts, type, empty);

    // qualifiers can also be null and have null values
    kv = new ClientKeyValue(wrap(row), wrap(family), null, ts, type, null);
    validate(kv, row, family, empty, ts, type, empty);
    kv = new ClientKeyValue(row, family, null, ts, type, null);
    validate(kv, row, family, empty, ts, type, empty);
    kv = new ClientKeyValue(row, family, null, ts, type);
    validate(kv, row, family, empty, ts, type, empty);
    // and also have values
    byte[] value = Bytes.toBytes("value");
    kv = new ClientKeyValue(wrap(row), wrap(family), null, ts, type, wrap(value));
    validate(kv, row, family, empty, ts, type, value);
    kv = new ClientKeyValue(row, family, null, ts, type, value);
    validate(kv, row, family, empty, ts, type, value);

    // families can also be null
    kv = new ClientKeyValue(wrap(row), null, null, ts, type, null);
    validate(kv, row, empty, empty, ts, type, empty);
    kv = new ClientKeyValue(row, null, null, ts, type, null);
    validate(kv, row, empty, empty, ts, type, empty);
    kv = new ClientKeyValue(row, null, null, ts, type);
    validate(kv, row, empty, empty, ts, type, empty);
    // but we could have a qualifier
    kv = new ClientKeyValue(wrap(row), null, wrap(qualifier), ts, type, null);
    validate(kv, row, empty, qualifier, ts, type, empty);
    kv = new ClientKeyValue(row, null, qualifier, ts, type, null);
    validate(kv, row, empty, qualifier, ts, type, empty);
    kv = new ClientKeyValue(row, null, qualifier, ts, type);
    validate(kv, row,  empty, qualifier, ts, type, empty);
    // or a real value
    kv = new ClientKeyValue(wrap(row), null, wrap(qualifier), ts, type, wrap(value));
    validate(kv, row, empty, qualifier, ts, type, value);
    kv = new ClientKeyValue(row, null, qualifier, ts, type, value);
    validate(kv, row, empty, qualifier, ts, type, value);
  }

  private void validate(KeyValue kv, byte[] row, byte[] family, byte[] qualifier, long ts,
      Type type, byte[] value) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    KeyValue.write(kv, out);
    out.close();
    byte[] data = out.getData();
    // read it back in
    DataInputBuffer in = new DataInputBuffer();
    in.reset(data, data.length);
    KeyValue read = KeyValue.create(in);
    in.close();

    // validate that its the same
    assertTrue("Row didn't match!", Bytes.equals(row, read.getRow()));
    assertTrue("Family didn't match!", Bytes.equals(family, read.getFamily()));
    assertTrue("Qualifier didn't match!", Bytes.equals(qualifier, read.getQualifier()));
    assertTrue("Value didn't match!", Bytes.equals(value, read.getValue()));
    assertEquals("Timestamp didn't match", ts, read.getTimestamp());
    assertEquals("Type didn't match", type.getCode(), read.getType());
  }

  /**
   * Copied from {@link TestKeyValue}
   * @throws Exception
   */
  @Test
  public void testColumnCompare() throws Exception {
    final byte [] a = Bytes.toBytes("aaa");
    byte [] family1 = Bytes.toBytes("abc");
    byte [] qualifier1 = Bytes.toBytes("def");
    byte [] family2 = Bytes.toBytes("abcd");
    byte [] qualifier2 = Bytes.toBytes("ef");

    KeyValue aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    assertFalse(aaa.matchingColumn(family2, qualifier2));
    assertTrue(aaa.matchingColumn(family1, qualifier1));
    aaa = new ClientKeyValue(a, family2, qualifier2, 0L, Type.Put, a);
    assertFalse(aaa.matchingColumn(family1, qualifier1));
    assertTrue(aaa.matchingColumn(family2,qualifier2));
    byte [] nullQualifier = new byte[0];
    aaa = new ClientKeyValue(a, family1, nullQualifier, 0L, Type.Put, a);
    assertTrue(aaa.matchingColumn(family1,null));
    assertFalse(aaa.matchingColumn(family2,qualifier2));
  }

  /**
   * Test a corner case when the family qualifier is a prefix of the column qualifier.
   */
  @Test
  public void testColumnCompare_prefix() throws Exception {
    final byte[] a = Bytes.toBytes("aaa");
    byte[] family1 = Bytes.toBytes("abc");
    byte[] qualifier1 = Bytes.toBytes("def");
    byte[] family2 = Bytes.toBytes("ab");
    byte[] qualifier2 = Bytes.toBytes("def");

    KeyValue aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    assertFalse(aaa.matchingColumn(family2, qualifier2));
  }

  /**
   * Test that we have the expected behavior when adding to a {@link Put}
   * @throws Exception
   */
  @Test
  public void testUsableWithPut() throws Exception {
    final byte[] a = Bytes.toBytes("aaa");
    byte[] family1 = Bytes.toBytes("abc");
    byte[] qualifier1 = Bytes.toBytes("def");

    // works fine - matching row
    KeyValue aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    Put p = new Put(a);
    p.add(aaa);

    // fails, not a matching row
    try {
      aaa = new ClientKeyValue(family1, family1, qualifier1, 0L, Type.Put, a);
      p.add(aaa);
      fail("Shouldn't have been able to add  a KV with a row mismatch");
    } catch (IOException e) {
      // noop - as expected
    }
  }

  /**
   * Test that we have the expected behavior when adding to a {@link Delete}
   * @throws Exception
   */
  @Test
  public void testUsableWithDelete() throws Exception {
    final byte[] a = Bytes.toBytes("aaa");
    byte[] family1 = Bytes.toBytes("abc");
    byte[] qualifier1 = Bytes.toBytes("def");

    KeyValue aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.Delete, a);
    Delete d = new Delete(a);
    // simple cases should work fine
    d.addDeleteMarker(aaa);
    aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.DeleteColumn, a);
    d.addDeleteMarker(aaa);
    aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.DeleteFamily, a);
    d.addDeleteMarker(aaa);

    // fails, not a matching row
    try {
      aaa = new ClientKeyValue(family1, family1, qualifier1, 0L, Type.DeleteFamily, a);
      d.addDeleteMarker(aaa);
      fail("Shouldn't have been able to add  a KV with a row mismatch");
    } catch (IOException e) {
      // noop - as expected
    }

    aaa = new ClientKeyValue(a, family1, qualifier1, 0L, Type.Put, a);
    try {
      d.addDeleteMarker(aaa);
      fail("Shouldn't have been able to add a KV of type Put");
    } catch (IOException e) {
      // noop
    }
  }

  private static ImmutableBytesWritable wrap(byte[] b) {
    return new ImmutableBytesWritable(b);
  }
}