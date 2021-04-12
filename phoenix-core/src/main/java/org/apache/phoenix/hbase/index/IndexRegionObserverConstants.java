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
package org.apache.phoenix.hbase.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;

public class IndexRegionObserverConstants {
    protected static final byte VERIFIED_BYTE = 1;
    protected static final byte UNVERIFIED_BYTE = 2;

    public static final byte[] UNVERIFIED_BYTES = new byte[] { UNVERIFIED_BYTE };
    public static final byte[] VERIFIED_BYTES = new byte[] { VERIFIED_BYTE };

    public static final String PHOENIX_APPEND_METADATA_TO_WAL = "phoenix.append.metadata.to.wal";
    public static final boolean DEFAULT_PHOENIX_APPEND_METADATA_TO_WAL = false;
    
    public static void removeEmptyColumn(Mutation m, byte[] emptyCF, byte[] emptyCQ) {
        List<Cell> cellList = m.getFamilyCellMap().get(emptyCF);
        if (cellList == null) {
            return;
        }
        Iterator<Cell> cellIterator = cellList.iterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    emptyCQ, 0, emptyCQ.length) == 0) {
                cellIterator.remove();
                return;
            }
        }
    }
    
    public static long getMaxTimestamp(Mutation m) {
        long maxTs = 0;
        long ts;
        for (List<Cell> cells : m.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                ts = cell.getTimestamp();
                if (ts > maxTs) {
                    maxTs = ts;
                }
            }
        }
        return maxTs;
    }
    
    /**
     * Enable indexing on the given table
     * @param descBuilder {@link TableDescriptor} for the table on which indexing should be enabled
   * @param builder class to use when building the index for this table
   * @param properties map of custom configuration options to make available to your
     *          {@link IndexBuilder} on the server-side
   * @param priority TODO
     * @throws IOException the Indexer coprocessor cannot be added
     */
    public static void enableIndexing(TableDescriptorBuilder descBuilder, String indexBuilderClassName,
        Map<String, String> properties, int priority) throws IOException {
      if (properties == null) {
        properties = new HashMap<String, String>();
      }
      properties.put(IndexerStatic.INDEX_BUILDER_CONF_KEY, indexBuilderClassName);
      descBuilder.addCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME, null, priority, properties);
    }
}
