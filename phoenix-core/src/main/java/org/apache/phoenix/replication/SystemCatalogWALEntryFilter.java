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
package org.apache.phoenix.replication;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

import java.util.List;

/**
 * Standard replication of the SYSTEM.CATALOG table can be dangerous because schemas
 * may change between the source and target clusters at different times, in particular
 * during cluster upgrades. However, tenant-owned data such as tenant-owned views need to
 * be copied. This WALEntryFilter will only allow tenant-owned rows in SYSTEM.CATALOG to
 * be replicated. Data from all other tables is automatically passed. It will also copy
 * child links in SYSTEM.CATALOG that are globally-owned but point to tenant-owned views.
 *
 */
public class SystemCatalogWALEntryFilter implements WALEntryFilter {

  private static byte[] CHILD_TABLE_BYTES =
      new byte[]{PTable.LinkType.CHILD_TABLE.getSerializedValue()};

  @Override
  public WAL.Entry filter(WAL.Entry entry) {

    //if the WAL.Entry's table isn't System.Catalog, it auto-passes this filter
    //TODO: when Phoenix drops support for pre-1.3 versions of HBase, redo as a WALCellFilter
    if (!SchemaUtil.isMetaTable(entry.getKey().getTablename().getName())){
      return entry;
    }

    List<Cell> cells = entry.getEdit().getCells();
    List<Cell> cellsToRemove = Lists.newArrayList();
    for (Cell cell : cells) {
      if (!isTenantRowCell(cell)){
        cellsToRemove.add(cell);
      }
    }
    cells.removeAll(cellsToRemove);
    if (cells.size() > 0) {
      return entry;
    } else {
      return null;
    }
  }

  private boolean isTenantRowCell(Cell cell) {
    ImmutableBytesWritable key =
        new ImmutableBytesWritable(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    //rows in system.catalog that aren't tenant-owned will have a leading separator byte
    boolean isTenantRowCell = key.get()[key.getOffset()] != QueryConstants.SEPARATOR_BYTE;

    /* In addition to the tenant view rows, there are parent-child links (see PHOENIX-2051) that
     * provide an efficient way for a parent table or view to look up its children.
     * These rows override SYSTEM_CATALOG.COLUMN_NAME with the child tenant_id,
     * if any, and contain only a single Cell, LINK_TYPE, which is of PTable.LinkType.Child
     */
    boolean isChildLinkToTenantView = false;
    if (!isTenantRowCell) {
      ImmutableBytesWritable columnQualifier = new ImmutableBytesWritable(cell.getQualifierArray(),
          cell.getQualifierOffset(), cell.getQualifierLength());
      boolean isChildLink = columnQualifier.compareTo(PhoenixDatabaseMetaData.LINK_TYPE_BYTES) == 0;
      if (isChildLink) {
        ImmutableBytesWritable columnValue = new ImmutableBytesWritable(cell.getValueArray(),
            cell.getValueOffset(), cell.getValueLength());
        if (columnValue.compareTo(CHILD_TABLE_BYTES) == 0) {
          byte[][] rowViewKeyMetadata = new byte[5][];
          SchemaUtil.getVarChars(key.get(), key.getOffset(),
              key.getLength(), 0, rowViewKeyMetadata);
          //if the child link is to a tenant-owned view,
          // the COLUMN_NAME field will be the byte[] of the tenant
          //otherwise, it will be an empty byte array
          // (NOT QueryConstants.SEPARATOR_BYTE, but a byte[0])
          isChildLinkToTenantView =
              rowViewKeyMetadata[PhoenixDatabaseMetaData.COLUMN_NAME_INDEX].length != 0;
        }
      }

    }
    return isTenantRowCell || isChildLinkToTenantView;
  }
}
