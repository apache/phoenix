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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.replication.WALCellFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME_INDEX;
/**
 * Standard replication of the SYSTEM.CATALOG and SYSTEM.CHILD_LINK table can
 * be dangerous because schemas may change between the source and target
 * clusters at different times, in particular during cluster upgrades.
 * However, tenant-owned data such as tenant-owned views need to be copied.
 * This WALEntryFilter will only allow tenant-owned rows in SYSTEM.CATALOG to
 * be replicated. Data from all other tables is automatically passed.
 */
public class SystemCatalogWALEntryFilter implements
    WALEntryFilter, WALCellFilter {
  /**
   * This is an optimization to just skip the cell filter if we do not care
   * about cell filter for certain WALEdits.
   */
  private boolean skipCellFilter;
  // Column value for parent child link
  private static final byte[] CHILD_TABLE_BYTES =
    new byte[]{PTable.LinkType.CHILD_TABLE.getSerializedValue()};
  // Number of columns in the primary key of system child link table
  private static final int NUM_COLUMNS_PRIMARY_KEY = 5;

  @Override
  public WAL.Entry filter(WAL.Entry entry) {
    // We use the WALCellFilter to filter the cells from entry, WALEntryFilter
    // should not block anything.
    // If the WAL.Entry's table isn't System.Catalog or System.Child_Link,
    // it auto-passes this filter.
    skipCellFilter =
      !(SchemaUtil.isMetaTable(entry.getKey().getTableName().getName())
      || SchemaUtil.isChildLinkTable(entry.getKey().getTableName().getName()));
    return entry;
  }

  @Override
  public Cell filterCell(final WAL.Entry entry, final Cell cell) {
    if (skipCellFilter) {
      return cell;
    }

    if (SchemaUtil.isMetaTable(entry.getKey().getTableName().getName())) {
      return isTenantIdLeadingInKey(cell) ? cell : null;
    } else {
      return isTenantRowCellSystemChildLink(cell) ? cell : null;
    }
  }

  /**
   * Does the cell key have leading tenant Id.
   * @param cell hbase cell
   * @return true if the cell has leading tenant Id in key
   */
  private boolean isTenantIdLeadingInKey(final Cell cell) {
    // rows in system.catalog or system child that aren't tenant-owned
    // will have a leading separator byte
    return cell.getRowArray()[cell.getRowOffset()]
      != QueryConstants.SEPARATOR_BYTE;
  }

  /**
   * is the cell for system child link a tenant owned. Besides the non empty
   * tenant id, system.child_link table have tenant owned data for parent child
   * links. In this case, the column qualifier is
   * {@code PhoenixDatabaseMetaData#LINK_TYPE_BYTES} and value is
   * {@code PTable.LinkType.CHILD_TABLE}. For corresponding delete markers the
   * KeyValue type {@code KeyValue.Type} is {@code KeyValue.Type.DeleteFamily}
   * @param cell hbase cell
   * @return true if the cell is tenant owned
   */
  private boolean isTenantRowCellSystemChildLink(final Cell cell) {
    boolean isTenantRowCell = isTenantIdLeadingInKey(cell);

    ImmutableBytesWritable key = new ImmutableBytesWritable(
      cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    boolean isChildLinkToTenantView = false;
    if (!isTenantRowCell) {
      boolean isChildLink = CellUtil.matchingQualifier(
        cell, PhoenixDatabaseMetaData.LINK_TYPE_BYTES);
      if ((isChildLink && CellUtil.matchingValue(cell, CHILD_TABLE_BYTES)) ||
          CellUtil.isDeleteFamily(cell)) {
        byte[][] rowViewKeyMetadata = new byte[NUM_COLUMNS_PRIMARY_KEY][];
        SchemaUtil.getVarChars(key.get(), key.getOffset(),
            key.getLength(), 0, rowViewKeyMetadata);
        /** if the child link is to a tenant-owned view, the COLUMN_NAME field will be
         * the byte[] of the tenant otherwise, it will be an empty byte array
         * (NOT QueryConstants.SEPARATOR_BYTE, but a byte[0]). This assumption is also
         * true for child link's delete markers in SYSTEM.CHILD_LINK as it only contains link
         * rows and does not deal with other type of rows like column rows that also has
         * COLUMN_NAME populated with actual column name.**/
        isChildLinkToTenantView = rowViewKeyMetadata[COLUMN_NAME_INDEX].length != 0;
      }
    }
    return isTenantRowCell || isChildLinkToTenantView;
  }
}
