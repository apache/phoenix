/**
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
package org.apache.phoenix.mapreduce.index;

/**
 * Counters used for Index Scrutiny MR job
 */
public enum PhoenixScrutinyJobCounters {
    /**
     * number of rows in data table with a valid index row (or vice-versa)
     */
    VALID_ROW_COUNT,
    /**
     * number of rows in data table with an invalid index row (or vice-versa)
     */
    INVALID_ROW_COUNT,
    /**
     * Number of rows in the index table with an incorrect covered column value
     */
    BAD_COVERED_COL_VAL_COUNT,
    /**
     * Number of batches processed
     */
    BATCHES_PROCESSED_COUNT;
}
