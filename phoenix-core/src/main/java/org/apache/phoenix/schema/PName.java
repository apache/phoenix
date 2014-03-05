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

import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;


/**
 * 
 * Interface to encapsulate both the client-side name
 * together with the server-side name for a named object
 *
 * 
 * @since 0.1
 */
public interface PName {
    public static PName EMPTY_NAME = new PName() {
        @Override
        public String getString() {
            return "";
        }

        @Override
        public byte[] getBytes() {
            return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        
        @Override
        public String toString() {
            return getString();
        }

        @Override
        public ImmutableBytesPtr getBytesPtr() {
            return ByteUtil.EMPTY_BYTE_ARRAY_PTR;
        }

        @Override
        public int getEstimatedSize() {
            return 0;
        }
    };
    public static PName EMPTY_COLUMN_NAME = new PName() {
        @Override
        public String getString() {
            return QueryConstants.EMPTY_COLUMN_NAME;
        }

        @Override
        public byte[] getBytes() {
            return QueryConstants.EMPTY_COLUMN_BYTES;
        }
        
        @Override
        public String toString() {
            return getString();
        }

        @Override
        public ImmutableBytesPtr getBytesPtr() {
            return QueryConstants.EMPTY_COLUMN_BYTES_PTR;
        }

        @Override
        public int getEstimatedSize() {
            return 0;
        }
    };
    /**
     * Get the client-side, normalized name as referenced
     * in a SQL statement.
     * @return the normalized string name
     */
    String getString();
    
    /**
     * Get the server-side name as referenced in HBase-related
     * APIs such as Scan, Filter, etc.
     * @return the name as a byte array
     */
    byte[] getBytes();

    /**
     * @return a pointer to the underlying bytes
     */
    ImmutableBytesPtr getBytesPtr();
    
    int getEstimatedSize();
}
