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
 * Counters used for Index Tool MR job
 */
public enum PhoenixIndexToolJobCounters {
    SCANNED_DATA_ROW_COUNT,
    REBUILT_INDEX_ROW_COUNT,
    BEFORE_REBUILD_VALID_INDEX_ROW_COUNT,
    BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT,
    BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT,
    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT,
    BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT,
    BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT,
    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS,
    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS,
    BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT,
    BEFORE_REBUILD_OLD_INDEX_ROW_COUNT,
    BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT,
    BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT,
    BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT,
    AFTER_REBUILD_VALID_INDEX_ROW_COUNT,
    AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT,
    AFTER_REBUILD_MISSING_INDEX_ROW_COUNT,
    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT,
    AFTER_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT,
    AFTER_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT,
    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS,
    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS,
    AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT,
    AFTER_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT
}
