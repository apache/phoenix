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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.annotation.Immutable;

import org.apache.phoenix.query.QueryConstants;

public class PNameFactory {

    private PNameFactory() {
    }

    public static PName newName(String name) {
        return name == null || name.isEmpty() ? PName.EMPTY_NAME : 
            name.equals(QueryConstants.EMPTY_COLUMN_NAME ) ?  PName.EMPTY_COLUMN_NAME : 
                new PNameImpl(name);
    }
    
    public static PName newName(byte[] bytes) {
        return bytes == null || bytes.length == 0 ? PName.EMPTY_NAME : 
            Bytes.compareTo(bytes, QueryConstants.EMPTY_COLUMN_BYTES) == 0 ? PName.EMPTY_COLUMN_NAME :
                new PNameImpl(bytes);
    }
    
    @Immutable
    private static class PNameImpl implements PName {
        private final String stringName;
        private final byte[] bytesName;
        
        private PNameImpl(String name) {
            this.stringName = name;
            this.bytesName = Bytes.toBytes(name);
        }
        
        private PNameImpl(byte[] name) {
            this.stringName = Bytes.toString(name);
            this.bytesName = name;
        }
        
        @Override
        public String getString() {
            return stringName;
        }

        @Override
        public byte[] getBytes() {
            return bytesName;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + stringName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            PNameImpl other = (PNameImpl)obj;
            // Compare normalized stringName for equality, since bytesName
            // may differ since it remains case sensitive.
            if (!stringName.equals(other.stringName)) return false;
            return true;
        }

        @Override
        public String toString() {
            return stringName;
        }
    }
}
