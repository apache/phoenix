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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PROTOBUF_DESCRIPTOR_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

/** Builds the {@code SYSTEM.PROTO_DESC_LINK} mutations that track PROTOBUF column usage. */
final class ProtoDescriptorLinkMutations {

  private ProtoDescriptorLinkMutations() {
  }

  /**
   * Returns one link Put per PROTOBUF column row in {@code catalogMutations}. If there are any, the
   * catalog Put cells still at LATEST_TIMESTAMP are set to {@code now}, so links and catalog rows
   * share one timestamp.
   */
  static List<Mutation> buildPuts(List<Mutation> catalogMutations, long now) throws IOException {
    List<Mutation> links = new ArrayList<>();
    for (Mutation m : catalogMutations) {
      List<Cell> cells = m instanceof Put
        ? ((Put) m).get(TABLE_FAMILY_BYTES, PROTOBUF_DESCRIPTOR_NAME_BYTES)
        : Collections.emptyList();
      if (!cells.isEmpty() && cells.get(0).getValueLength() > 0) {
        // tenant, schema, table, column, family
        byte[][] key = new byte[5][];
        SchemaUtil.getVarChars(m.getRow(), key);
        long ts = cells.get(0).getTimestamp();
        byte[] linkKey = MetaDataUtil.getProtoDescLinkKey(CellUtil.cloneValue(cells.get(0)), key[0],
          key[1], key[2], key[4], key[3]);
        links.add(new Put(linkKey).addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
          QueryConstants.EMPTY_COLUMN_BYTES, ts == HConstants.LATEST_TIMESTAMP ? now : ts,
          QueryConstants.EMPTY_COLUMN_VALUE_BYTES));
      }
    }
    if (!links.isEmpty()) {
      for (Mutation m : catalogMutations) {
        if (m instanceof Put) {
          for (List<Cell> family : m.getFamilyCellMap().values()) {
            for (Cell cell : family) {
              PrivateCellUtil.updateLatestStamp(cell, now);
            }
          }
        }
      }
    }
    return links;
  }

  static List<Mutation> buildDeletesForTable(PTable table, long timestamp) {
    List<Mutation> deletes = new ArrayList<>();
    for (PColumn column : table.getColumns()) {
      addDelete(deletes, table, column, timestamp);
    }
    return deletes;
  }

  static void addDelete(List<Mutation> deletes, PTable table, PColumn column, long timestamp) {
    if (isLinked(column)) {
      deletes.add(
        new Delete(MetaDataUtil.getProtoDescLinkKey(Bytes.toBytes(column.getProtoMessageName()),
          bytes(table.getTenantId()), bytes(table.getSchemaName()), table.getTableName().getBytes(),
          bytes(column.getFamilyName()), column.getName().getBytes()), timestamp));
    }
  }

  /** Derived columns are skipped: their link belongs to the ancestor that declared them. */
  private static boolean isLinked(PColumn column) {
    return column.getDataType() == PProtobuf.INSTANCE && !column.isDerived()
      && column.getProtoMessageName() != null && !column.getProtoMessageName().isEmpty();
  }

  private static byte[] bytes(PName name) {
    return name == null ? null : name.getBytes();
  }
}
