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
package org.apache.phoenix.client;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

/**
 * A {@link KeyValueBuilder} that builds {@link ClientKeyValue}, eliminating the extra byte copies
 * inherent in the standard {@link KeyValue} implementation.
 * <p>
 * This {@link KeyValueBuilder} is only supported in HBase 0.94.14+ (
 * {@link PhoenixDatabaseMetaData#CLIENT_KEY_VALUE_BUILDER_THRESHOLD}), with the addition of
 * HBASE-9834.
 */
public class ClientKeyValueBuilder extends KeyValueBuilder {

    public static final KeyValueBuilder INSTANCE = new ClientKeyValueBuilder();

  private ClientKeyValueBuilder() {
    // private ctor for singleton
  }

  @Override
  public KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, long ts, ImmutableBytesWritable value) {
    return new ClientKeyValue(row, family, qualifier, ts, Type.Put, value);
  }

  @Override
  public KeyValue buildDeleteFamily(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.DeleteFamily, null);
  }

  @Override
  public KeyValue buildDeleteColumns(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.DeleteColumn, null);
  }

  @Override
  public KeyValue buildDeleteColumn(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier, long ts) {
        return new ClientKeyValue(row, family, qualifier, ts, Type.Delete, null);
  }

  @Override
  public int compareQualifier(KeyValue kv, byte[] key, int offset, int length) {
        byte[] qual = kv.getQualifier();
        return Bytes.compareTo(qual, 0, qual.length, key, offset, length);
  }

  @Override
  public void getValueAsPtr(KeyValue kv, ImmutableBytesWritable ptr) {
        ClientKeyValue ckv = (ClientKeyValue) kv;
        ImmutableBytesWritable value = ckv.getRawValue();
        ptr.set(value.get(), value.getOffset(), value.getLength());
  }
}