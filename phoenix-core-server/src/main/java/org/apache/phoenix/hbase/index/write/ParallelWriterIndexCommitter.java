/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.hbase.index.write;

import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.parallel.EarlyExitFailure;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

/**
 * Write index updates to the index tables in parallel. We attempt to early exit from the writes if any of the index
 * updates fails. Completion is determined by the following criteria: *
 * <ol>
 * <li>All index writes have returned, OR</li>
 * <li>Any single index write has failed</li>
 * </ol>
 * We attempt to quickly determine if any write has failed and not write to the remaining indexes to ensure a timely
 * recovery of the failed index writes.
 */
public class ParallelWriterIndexCommitter extends AbstractParallelWriterIndexCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelWriterIndexCommitter.class);


    public ParallelWriterIndexCommitter() {}

    // For testing
    public ParallelWriterIndexCommitter(String hbaseVersion) {
        super(hbaseVersion);
    }



    @Override
    public void write(Multimap<HTableInterfaceReference, Mutation> toWrite, final boolean allowLocalUpdates, final int clientVersion) throws SingleIndexWriteFailureException {

        super.write(toWrite, allowLocalUpdates, clientVersion);
        // actually submit the tasks to the pool and wait for them to finish/fail
        try {
            pool.submitUninterruptible(tasks);
        } catch (EarlyExitFailure e) {
            propagateFailure(e);
        } catch (ExecutionException e) {
            LOGGER.error("Found a failed index update!");
            propagateFailure(e.getCause());
        }

    }
}