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

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;

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
     * {@link #setValue(PColumn, byte[])} or {@link #delete()}.
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
    public void setValue(PColumn col, byte[] value);

    /**
     * Set attributes for the Put operations involving dynamic columns. These attributes are
     * persisted as cells under a reserved qualifier for the dynamic column metadata so that we
     * can resolve them for wildcard queries without requiring the user to provide the data type
     * of the dynamic columns. See PHOENIX-374
     * @return true if attributes for dynamic columns are added, otherwise false
     */
    public boolean setAttributesForDynamicColumnsIfReqd();

    /**
     * Set an attribute to indicate that we must process dynamic column metadata for the mutation.
     * This is set if the configuration for supporting dynamic columns in wildcard queries is on
     * and there are actually dynamic columns for which we need to add metadata.
     * In case of old clients or for clients where this configuration is off, or for clients where
     * this configuration is on and there are no dynamic columns to process in the mutation, this
     * attribute will not be set.
     * If this attribute is not set, we can avoid unnecessary iterations over each mutation's
     * column families. See
     * {@link org.apache.phoenix.coprocessor.ScanRegionObserver#preBatchMutate(ObserverContext,
     * MiniBatchOperationInProgress)}
     */
    public void setAttributeToProcessDynamicColumnsMetadata();

    /**
     * Delete the row. Note that a delete take precedence over any
     * values that may have been set before or after the delete call.
     */
    public void delete();
}
