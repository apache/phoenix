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

import java.util.Collection;

/**
 * 
 * Definition of a Phoenix Column Family
 *
 * 
 * @since 0.1
 */
public interface PColumnFamily {
    
    /**
     * @return The column family name.
     */
    PName getName();
    
    /**
     * @return All the PColumns in this column family.
     */
    Collection<PColumn> getColumns();
    
    /**
     * @return The PColumn for the specified column qualifier.
     * @throws ColumnNotFoundException if the column cannot be found
     */
    PColumn getColumn(byte[] qualifier) throws ColumnNotFoundException;
    
    /**
     * @return The PColumn for the specified column qualifier.
     * @throws ColumnNotFoundException if the column cannot be found
     */
    PColumn getColumn(String name) throws ColumnNotFoundException;
    
    int getEstimatedSize();
}