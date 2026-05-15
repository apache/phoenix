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
package org.apache.phoenix.compile;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

/** Test helper that inserts a virtual PColumn into a PTable cached on the connection. */
public final class VirtualColumnTestUtil {
  private VirtualColumnTestUtil() {}

  public static void injectVirtualColumn(Connection conn, String tableName,
      String columnName, String typeName) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    PTable table = pconn.getTable(SchemaUtil.normalizeIdentifier(tableName));
    PName name = PNameFactory.newName(SchemaUtil.normalizeIdentifier(columnName));
    PName fam = table.getDefaultFamilyName() != null
        ? table.getDefaultFamilyName() : PNameFactory.newName("0");
    PDataType<?> type = PDataType.fromSqlTypeName(typeName);
    int position = table.getColumns().size();
    PColumnImpl virtual = new PColumnImpl(name, fam, type, null, null, true,
        position, SortOrder.getDefault(), null, null, false, null, false, false,
        name.getBytes(), 0L, false, /* isVirtual = */ true);

    List<PColumn> cols = new ArrayList<>(table.getColumns());
    cols.add(virtual);
    PTable updated = PTableImpl.builderWithColumns(table, cols).build();
    pconn.addTable(updated, System.currentTimeMillis());
  }
}
