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
package org.apache.phoenix.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * This filter overrides the behavior of delegate so that we do not jump to the next
 * column as soon as we find a value for a column but rather include all versions which is
 * needed for rebuilds.
 */
public class AllVersionsIndexRebuildFilter extends DelegateFilter {

    public AllVersionsIndexRebuildFilter(Filter originalFilter) {
        super(originalFilter);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        ReturnCode delegateCode = super.filterKeyValue(v);
        if (delegateCode == ReturnCode.INCLUDE_AND_NEXT_COL) {
            return ReturnCode.INCLUDE;
        } else {
            return delegateCode;
        }
    }
}
