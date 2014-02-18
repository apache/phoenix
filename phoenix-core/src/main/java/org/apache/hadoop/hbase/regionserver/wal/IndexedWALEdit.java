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
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.hbase.index.wal.KeyValueCodec;

/**
 * Read in data for a delegate {@link WALEdit}. This should only be used in concert with an IndexedHLogReader
 * <p>
 * This class should only be used with HBase &lt; 0.94.9. Newer installations of HBase should
 * instead use the IndexedWALEditCodec along with the correct configuration options.
 */
public class IndexedWALEdit extends WALEdit {
  //reproduced here so we don't need to modify the HBase source.
  private static final int VERSION_2 = -1;
  private WALEdit delegate;

  /**
   * Copy-constructor. Only does a surface copy of the delegates fields - no actual data is copied, only referenced.
   * @param delegate to copy
   */
  @SuppressWarnings("deprecation")
  public IndexedWALEdit(WALEdit delegate) {
    this.delegate = delegate;
    // reset the delegate's fields
    this.delegate.getKeyValues().clear();
    if (this.delegate.getScopes() != null) {
      this.delegate.getScopes().clear();
    }
  }

  public IndexedWALEdit() {

  }

  @Override
public void setCompressionContext(CompressionContext context) {
    throw new UnsupportedOperationException(
        "Compression not supported for IndexedWALEdit! If you are using HBase 0.94.9+, use IndexedWALEditCodec instead.");
  }

  @SuppressWarnings("deprecation")
  @Override
  public void readFields(DataInput in) throws IOException {
    delegate.getKeyValues().clear();
    if (delegate.getScopes() != null) {
      delegate.getScopes().clear();
    }
    // ----------------------------------------------------------------------------------------
    // no compression, so we do pretty much what the usual WALEdit does, plus a little magic to
    // capture the index updates
    // -----------------------------------------------------------------------------------------
    int versionOrLength = in.readInt();
    if (versionOrLength != VERSION_2) {
      throw new IOException("You must update your cluster to the lastest version of HBase and"
          + " clean out all logs (cleanly start and then shutdown) before enabling indexing!");
    }
    // this is new style HLog entry containing multiple KeyValues.
    List<KeyValue> kvs = KeyValueCodec.readKeyValues(in);
    delegate.getKeyValues().addAll(kvs);

    // then read in the rest of the WALEdit
    int numFamilies = in.readInt();
    NavigableMap<byte[], Integer> scopes = delegate.getScopes();
    if (numFamilies > 0) {
      if (scopes == null) {
        scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      }
      for (int i = 0; i < numFamilies; i++) {
        byte[] fam = Bytes.readByteArray(in);
        int scope = in.readInt();
        scopes.put(fam, scope);
      }
      delegate.setScopes(scopes);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new IOException(
        "Indexed WALEdits aren't written directly out - use IndexedKeyValues instead");
  }
}