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
import org.apache.hadoop.hbase.replication.WALCellFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.SchemaUtil;


/**
 * Standard replication of the SYSTEM.CATALOG table can be dangerous because schemas
 * may change between the source and target clusters at different times, in particular
 * during cluster upgrades. However, tenant-owned data such as tenant-owned views need to
 * be copied. This WALEntryFilter will only allow tenant-owned rows in SYSTEM.CATALOG to
 * be replicated. Data from all other tables is automatically passed.
 */
public class SystemCatalogWALEntryFilter implements
    WALEntryFilter, WALCellFilter {
  /**
   * This is an optimization to just skip the cell filter if we do not care
   * about cell filter for certain WALEdits.
   */
  private boolean skipCellFilter;

  @Override
  public WAL.Entry filter(WAL.Entry entry) {
    // We use the WALCellFilter to filter the cells from entry, WALEntryFilter
    // should not block anything
    // if the WAL.Entry's table isn't System.Catalog or System.Child_Link,
    // it auto-passes this filter
    if (!SchemaUtil.isMetaTable(entry.getKey().getTableName().getName())){
      skipCellFilter = true;
    } else {
      skipCellFilter = false;
    }
    return entry;
  }

  @Override
  public Cell filterCell(final WAL.Entry entry, final Cell cell) {
    if (skipCellFilter) {
      return cell;
    }
    return isTenantRowCell(cell) ? cell : null;
  }

  private boolean isTenantRowCell(Cell cell) {
    // rows in system.catalog that aren't tenant-owned 
    // will have a leading separator byte
    return cell.getRowArray()[cell.getRowOffset()]
        != QueryConstants.SEPARATOR_BYTE;
  }
}
