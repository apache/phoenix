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
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;

import org.apache.phoenix.util.SchemaUtil;


public class MetaDataSplitPolicy extends ConstantSizeRegionSplitPolicy {

    @Override
    protected byte[] getSplitPoint() {
        byte[] splitPoint = super.getSplitPoint();
        int offset = SchemaUtil.getVarCharLength(splitPoint, 0, splitPoint.length);
        // Split only on Phoenix schema name, so this is ok b/c we won't be splitting
        // in the middle of a Phoenix table.
        if (offset == splitPoint.length) {
            return splitPoint;
        }
//        offset = SchemaUtil.getVarCharLength(splitPoint, offset+1, splitPoint.length-offset-1);
//        // Split only on Phoenix schema and table name, so this is ok b/c we won't be splitting
//        // in the middle of a Phoenix table.
//        if (offset == splitPoint.length) {
//            return splitPoint;
//        }
        // Otherwise, an attempt is being made to split in the middle of a table.
        // Just return a split point at the schema boundary instead
        byte[] newSplitPoint = new byte[offset + 1];
        System.arraycopy(splitPoint, 0, newSplitPoint, 0, offset+1);
        return newSplitPoint;
    }

}
