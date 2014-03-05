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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Mutation;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * Provide a client API for updating rows. The updates are processed in
 * the calling order. Calling setValue after calling delete will cause the
 * delete to be canceled.  Conversely, calling delete after calling
 * setValue will cause all prior setValue calls to be canceled.
 *
 * 
 * @since 0.1
 */
public interface PRow {
    Map<PColumn, byte[]> DELETE_MARKER = ImmutableMap.of();

    /**
     * Get the list of {@link org.apache.hadoop.hbase.client.Mutation} used to
     * update an HTable after all mutations through calls to
     * {@link #setValue(PColumn, Object)} or {@link #delete()}.
     * @return the list of mutations representing all changes made to a row
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public List<Mutation> toRowMutations();
    
    /**
     * Set a column value in the row
     * @param col the column for which the value is being set
     * @param value the value
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public void setValue(PColumn col, Object value);
    
    /**
     * Set a column value in the row
     * @param col the column for which the value is being set
     * @param value the value
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    public void setValue(PColumn col, byte[] value);
    
    /**
     * Delete the row. Note that a delete take precedence over any
     * values that may have been set before or after the delete call.
     */
    public void delete();
}
