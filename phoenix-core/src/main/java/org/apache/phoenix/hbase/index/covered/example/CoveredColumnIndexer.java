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

package org.apache.phoenix.hbase.index.covered.example;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.hbase.index.covered.Batch;
import org.apache.phoenix.hbase.index.covered.CoveredColumnsIndexBuilder;
import org.apache.phoenix.hbase.index.covered.LocalTableState;
import org.apache.phoenix.hbase.index.covered.update.IndexUpdateManager;

/**
 * Index maintainer that maintains multiple indexes based on '{@link ColumnGroup}s'. Each group is a
 * fully covered within itself and stores the fully 'pre-joined' version of that values for that
 * group of columns.
 * <p>
 * <h2>Index Layout</h2> The row key for a given index entry is the current state of the all the
 * values of the columns in a column group, followed by the primary key (row key) of the original
 * row, and then the length of each value and then finally the total number of values. This is then
 * enough information to completely rebuild the latest value of row for each column in the group.
 * <p>
 * The family is always {@link CoveredColumnIndexCodec#INDEX_ROW_COLUMN_FAMILY}
 * <p>
 * The qualifier is prepended with the integer index (serialized with {@link Bytes#toBytes(int)}) of
 * the column in the group. This index corresponds the index of the value for the group in the row
 * key.
 * 
 * <pre>
 *         ROW                            ||   FAMILY     ||    QUALIFIER     ||   VALUE
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     1Cf1:Cq1     ||  null
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     2Cf2:Cq2     ||  null
 * ...
 * (v1)(v2)...(vN)(pk)(L1)(L2)...(Ln)(#V) || INDEX_FAMILY ||     NCfN:CqN     ||  null
 * </pre>
 * 
 * <h2>Index Maintenance</h2>
 * <p>
 * When making an insertion into the table, we also attempt to cleanup the index. This means that we
 * need to remove the previous entry from the index. Generally, this is completed by inserting a
 * delete at the previous value of the previous row.
 * <p>
 * The main caveat here is when dealing with custom timestamps. If there is no special timestamp
 * specified, we can just insert the proper {@link Delete} at the current timestamp and move on.
 * However, when the client specifies a timestamp, we could see updates out of order. In that case,
 * we can do an insert using the specified timestamp, but a delete is different...
 * <p>
 * Taking the simple case, assume we do a single column in a group. Then if we get an out of order
 * update, we need to check the current state of that column in the current row. If the current row
 * is older, we can issue a delete as normal. If the current row is newer, however, we then have to
 * issue a delete for the index update at the time of the current row. This ensures that the index
 * update made for the 'future' time still covers the existing row.
 * <p>
 * <b>ASSUMPTION:</b> all key-values in a single {@link Delete}/{@link Put} have the same timestamp.
 * This dramatically simplifies the logic needed to manage updating the index for out-of-order
 * {@link Put}s as we don't need to manage multiple levels of timestamps across multiple columns.
 * <p>
 * We can extend this to multiple columns by picking the latest update of any column in group as the
 * delete point.
 * <p>
 * <b>NOTE:</b> this means that we need to do a lookup (point {@link Get}) of the entire row
 * <i>every time there is a write to the table</i>.
 */
public class CoveredColumnIndexer extends CoveredColumnsIndexBuilder {

  /**
   * Create the specified index table with the necessary columns
   * @param admin {@link HBaseAdmin} to use when creating the table
   * @param indexTable name of the index table.
   * @throws IOException
   */
  public static void createIndexTable(HBaseAdmin admin, String indexTable) throws IOException {
    createIndexTable(admin, new HTableDescriptor(indexTable));
  }

  /**
   * @param admin to create the table
   * @param index descriptor to update before creating table
   */
  public static void createIndexTable(HBaseAdmin admin, HTableDescriptor index) throws IOException {
    HColumnDescriptor col =
        new HColumnDescriptor(CoveredColumnIndexCodec.INDEX_ROW_COLUMN_FAMILY);
    // ensure that we can 'see past' delete markers when doing scans
    col.setKeepDeletedCells(true);
    index.addFamily(col);
    admin.createTable(index);
  }

  @Override
  public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {

    // stores all the return values
    IndexUpdateManager updateMap = new IndexUpdateManager();
    // batch the updates by row to make life easier and ordered
    Collection<Batch> batches = batchByRow(filtered);

    for (Batch batch : batches) {
      KeyValue curKV = batch.getKvs().iterator().next();
      Put p = new Put(curKV.getRowArray(), curKV.getRowOffset(), curKV.getRowLength());
      for (KeyValue kv : batch.getKvs()) {
        // we only need to cleanup Put entries
        byte type = kv.getTypeByte();
        Type t = KeyValue.Type.codeToType(type);
        if (!t.equals(Type.Put)) {
          continue;
        }

        // add the kv independently
        p.add(kv);
      }

      // do the usual thing as for deletes
      Collection<Batch> timeBatch = createTimestampBatchesFromMutation(p);
      LocalTableState state = new LocalTableState(env, localTable, p);
      for (Batch entry : timeBatch) {
        //just set the timestamp on the table - it already has all the future state
        state.setCurrentTimestamp(entry.getTimestamp());
        this.addDeleteUpdatesToMap(updateMap, state, entry.getTimestamp());
      }
    }
    return updateMap.toMap();
  }


  /**
   * @param filtered
   */
  private Collection<Batch>  batchByRow(Collection<KeyValue> filtered) {
    Map<Long, Batch> batches = new HashMap<Long, Batch>();
    createTimestampBatchesFromKeyValues(filtered, batches);
    return batches.values();
  }
}