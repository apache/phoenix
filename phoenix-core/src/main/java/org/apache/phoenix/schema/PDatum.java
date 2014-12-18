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

import org.apache.phoenix.schema.types.PDataType;

public interface PDatum {
    /**
     * @return is this column nullable?
     */
    boolean isNullable();

    /**
     * @return data type of the column
     */
    PDataType getDataType();

    /**
     * @return the actual length of the column. For decimal, it would be its precision. For char or
     * varchar, it would be the maximum length as specified during schema definition.
     */
    Integer getMaxLength();

    /**
     * @return scale of a decimal number.
     */
    Integer getScale();
    
    /**
     * @return The SortOrder for this column, never null
     */
    SortOrder getSortOrder();
}
