/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.annotation.Immutable;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.SizedUtil;

@Immutable
public class PNameImpl implements PName {
    /**
     */
    private static class PNameImplData {
        /**  */
        public String stringName;
        /**  */
        public byte[] bytesName;
        /**  */
        public ImmutableBytesPtr ptr;

        /**
         *
         */
        public PNameImplData() {
        }
    }
    private PNameImplData data = new PNameImplData();


    @Override
    public int getEstimatedSize() {
        return SizedUtil.OBJECT_SIZE * 3 + SizedUtil.ARRAY_SIZE + SizedUtil.IMMUTABLE_BYTES_PTR_SIZE +
                data.stringName.length() * SizedUtil.CHAR_SIZE + data.bytesName.length;
    }

    PNameImpl(String name) {
        this.data.stringName = name;
        this.data.bytesName = Bytes.toBytes(name);
    }

    PNameImpl(byte[] name) {
        this.data.stringName = Bytes.toString(name);
        this.data.bytesName = name;
    }

    @Override
    public String getString() {
        return data.stringName;
    }

    @Override
    public byte[] getBytes() {
        return data.bytesName;
    }

    @Override
    public ImmutableBytesPtr getBytesPtr() {
        if (data.ptr == null) {
            synchronized (data.bytesName) {
                if (data.ptr == null) {
                    this.data.ptr = new ImmutableBytesPtr(data.bytesName);
                }
            }
        }
        return this.data.ptr;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + data.stringName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (! (obj instanceof PName) ) return false;
        PName other = (PName)obj;
        if (hashCode() != other.hashCode()) return false;
        // Compare normalized stringName for equality, since bytesName
        // may differ since it remains case sensitive.
        if (!getString().equals(other.getString())) return false;
        return true;
    }

    @Override
    public String toString() {
        return data.stringName;
    }
}