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

import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;


/**
 * Interface to encode column values into a serialized byte[] that will be stored in a single cell
 * The last byte of the serialized byte[] should be the serialized value of the {@link ImmutableStorageScheme}
 * that was used.
 */
public interface ColumnValueEncoder {
    
    /**
     * append a column value to the array
     */
    void appendValue(byte[] bytes, int offset, int length);
    
    /**
     * append a value that is not present to the array (used to support DEFAULT expressions)
     */
    void appendAbsentValue();
    
    /**
     * @return the encoded byte[] that contains the serialized column values
     */
    byte[] encode();
    
}