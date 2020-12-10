/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowCalculator {
    private final int buckets;
    private final int rows;
    private final List<Integer> rowCountList;

    public RowCalculator(int buckets, int rows) {
        this.buckets = buckets;
        this.rows = rows;
        this.rowCountList = Collections.synchronizedList(new ArrayList<Integer>(buckets));
        init();
    }

    public synchronized int size() {
        return rowCountList.size();
    }

    public synchronized int getNext() {
        return rowCountList.remove(0);
    }

    /**
     * Get the number of row that should fit into each bucket.
     * @return
     */
    public int getRowsPerBucket() {
        return rows / buckets;
    }

    /**
     * Get the number of extra rows that need to be added if rows don't divide evenly among the buckets.
     * @return
     */
    public int getExtraRowCount() {
        return rows % buckets;
    }

    private void init() {
        for (int i = 0; i < buckets; i++) {
            synchronized (rowCountList) {
                // On the first row count we tack on the extra rows if they exist
                if (i == 0) {
                    // When row count is small we just put them all in the first bucket
                    if (rows < buckets) {
                        rowCountList.add(getExtraRowCount());
                    } else {
                        rowCountList.add(getRowsPerBucket() + getExtraRowCount());
                    }
                } else {
                    rowCountList.add(getRowsPerBucket());
                }
            }
        }
    }
}
