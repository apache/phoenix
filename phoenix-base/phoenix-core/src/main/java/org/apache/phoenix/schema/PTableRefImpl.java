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


public class PTableRefImpl extends PTableRef {
    
    private final PTable table;
    
    public PTableRefImpl(PTable table, long lastAccessTime, long resolvedTime, int estimatedSize) {
        super(lastAccessTime, resolvedTime, estimatedSize);
        this.table = table;
    }

    public PTableRefImpl(PTableRef tableRef) {
        super(tableRef.getLastAccessTime(), tableRef.getResolvedTimeStamp(), tableRef.getEstimatedSize());
        this.table = tableRef.getTable();
    }

    @Override
    public PTable getTable() {
        return table;
    }
}
