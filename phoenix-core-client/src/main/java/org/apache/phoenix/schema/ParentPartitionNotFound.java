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

/**
 * Exception thrown when a parent partition cannot be found in the CDC stream metadata.
 * This typically occurs during region split or merge operations when trying to update
 * the parent-daughter relationship in the CDC stream metadata.
 */
public class ParentPartitionNotFound extends RuntimeException {

    /**
     * Creates a new ParentPartitionNotFound exception with the specified error message.
     *
     * @param message the error message.
     */
    public ParentPartitionNotFound(String message) {
        super(message);
    }

}
