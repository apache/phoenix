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
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.SchemaUtil;

import java.util.List;

/**
 * Standard replication of the SYSTEM.CATALOG table can be dangerous because schemas
 * may change between the source and target clusters at different times, in particular
 * during cluster upgrades. However, tenant-owned data such as tenant-owned views need to
 * be copied. This WALEntryFilter will only allow tenant-owned rows in SYSTEM.CATALOG to
 * be replicated. Data from all other tables is automatically passed.
 */
public class SystemCatalogWALEntryFilter implements WALEntryFilter {

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
    return key.get()[key.getOffset()] != QueryConstants.SEPARATOR_BYTE;
  }
}
