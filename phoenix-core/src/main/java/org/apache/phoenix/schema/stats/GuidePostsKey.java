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
package org.apache.phoenix.schema.stats;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

/**
 * 
 * Key for the client-side caching of the guideposts information
 *
 */
public final class GuidePostsKey {
    private final int hashCode;
    @Nonnull private final byte[] physicalName;
    @Nonnull private final byte[] columnFamily;
    
    public GuidePostsKey(byte[] physicalName, byte[] columnFamily) {
        Preconditions.checkNotNull(physicalName);
        Preconditions.checkNotNull(columnFamily);
        this.physicalName = physicalName;
        this.columnFamily = columnFamily;
        this.hashCode = computeHashCode();
    }
    
    public byte[] getPhysicalName() {
        return physicalName;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
    
    private int computeHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(columnFamily);
        result = prime * result + Arrays.hashCode(physicalName);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        GuidePostsKey other = (GuidePostsKey)obj;
        if (other.hashCode != this.hashCode) return false;
        if (!Arrays.equals(columnFamily, other.columnFamily)) return false;
        if (!Arrays.equals(physicalName, other.physicalName)) return false;
        return true;
    }
    
    @Override
    public String toString() {
        return "GuidePostsKey[physicalName=" + Bytes.toStringBinary(physicalName) 
                + ",columnFamily=" + Bytes.toStringBinary(columnFamily) + "]";
    }
}
