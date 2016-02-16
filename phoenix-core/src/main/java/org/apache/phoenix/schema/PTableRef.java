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

public class PTableRef {
    private final PTable table;
    private final int estSize;
    private final long createTime;
    private final long resolvedTimeStamp;
	private volatile long lastAccessTime;
    
    public PTableRef(PTable table, long lastAccessTime, int estSize, long resolvedTime) {
        this.table = table;
        this.lastAccessTime = lastAccessTime;
        this.estSize = estSize;
        this.resolvedTimeStamp = resolvedTime;
        this.createTime = lastAccessTime;
    }

    public PTableRef(PTable table, long lastAccessTime, long resolvedTime) {
        this (table, lastAccessTime, table.getEstimatedSize(), resolvedTime);
    }

    public PTableRef(PTableRef tableRef) {
        this (tableRef.table, tableRef.lastAccessTime, tableRef.estSize, tableRef.resolvedTimeStamp);
    }
    
    /**
     * Tracks how long this entry has been in the cache
     * @return time in milliseconds for how long this entry has been in the cache.
     */
    public long getCreateTime() {
        return createTime;
    }
    
    public PTable getTable() {
		return table;
	}

	public long getResolvedTimeStamp() {
		return resolvedTimeStamp;
	}
	
    public int getEstSize() {
		return estSize;
	}

	public long getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(long lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}
}