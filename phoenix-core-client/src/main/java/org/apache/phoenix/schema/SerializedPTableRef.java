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
package org.apache.phoenix.schema;

import java.io.IOException;
import org.apache.phoenix.coprocessor.generated.PTableProtos;

public class SerializedPTableRef extends PTableRef {

  private final byte[] tableBytes;

  public SerializedPTableRef(byte[] tableBytes, long lastAccessTime, long resolvedTime,
    int estimatedSize) {
    super(lastAccessTime, resolvedTime, tableBytes.length);
    this.tableBytes = tableBytes;
  }

  public SerializedPTableRef(PTableRef tableRef) {
    super(tableRef.getCreateTime(), tableRef.getResolvedTimeStamp(), tableRef.getEstimatedSize());
    this.tableBytes = ((SerializedPTableRef) tableRef).tableBytes;
  }

  @Override
  public PTable getTable() {
    try {
      return PTableImpl.createFromProto(PTableProtos.PTable.parseFrom(tableBytes));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
