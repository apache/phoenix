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

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.SteppingSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;

/**
 * Split policy for local indexed tables to select split key from non local index column families
 * always.
 */
public class IndexRegionSplitPolicy extends SteppingSplitPolicy {

    @Override
    protected boolean skipStoreFileRangeCheck(String familyName) {
        if (familyName.startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
            return true;
        }
        return false;
    }

    @Override
    protected byte[] getSplitPoint() {
        byte[] oldSplitPoint = super.getSplitPoint();
        if (oldSplitPoint == null) return null;
        List<HStore> stores = region.getStores();
        byte[] splitPointFromLargestStore = null;
        long largestStoreSize = 0;
        boolean isLocalIndexKey = false;
        for (HStore s : stores) {
            if (s.getColumnFamilyName()
                    .startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                Optional<byte[]> splitPoint = s.getSplitPoint();
                if (oldSplitPoint != null && splitPoint.isPresent()
                        && Bytes.compareTo(oldSplitPoint, splitPoint.get()) == 0) {
                    isLocalIndexKey = true;
                }
            }
        }
        if (!isLocalIndexKey) return oldSplitPoint;

        for (HStore s : stores) {
            if (!s.getColumnFamilyName()
                    .startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                Optional<byte[]> splitPoint = s.getSplitPoint();
                long storeSize = s.getSize();
                if (splitPoint.isPresent() && largestStoreSize < storeSize) {
                    splitPointFromLargestStore = splitPoint.get();
                    largestStoreSize = storeSize;
                }
            }
        }
        return splitPointFromLargestStore;
    }
}
