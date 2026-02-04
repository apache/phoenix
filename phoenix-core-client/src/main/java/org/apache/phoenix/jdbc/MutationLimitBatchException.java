/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import java.sql.BatchUpdateException;
import org.apache.phoenix.schema.MutationLimitReachedException;

/**
 * Thrown from executeBatch() when the mutation buffer limit is reached.
 * The batch is automatically trimmed to contain only unprocessed items.
 */
public class MutationLimitBatchException extends BatchUpdateException {
    private static final long serialVersionUID = 1L;

    private final int processedCount;

    /**
     * @param updateCounts array of update counts for each statement in the batch
     * @param cause the underlying MutationLimitReachedException
     * @param processedCount number of statements successfully processed
     */
    public MutationLimitBatchException(
            int[] updateCounts,
            MutationLimitReachedException cause,
            int processedCount) {
        super(cause.getMessage(),
              cause.getSQLState(),
              cause.getErrorCode(),
              updateCounts,
              cause);
        this.processedCount = processedCount;
    }

    /**
     * Returns the number of statements that were successfully processed
     * before the limit was reached. The batch has been trimmed to contain
     * only the remaining unprocessed items.
     */
    public int getProcessedCount() {
        return processedCount;
    }
}
