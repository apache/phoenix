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
package org.apache.phoenix.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.hbase.index.covered.TxIndexBuilder;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.util.IndexUtil;

public class PhoenixTxIndexBuilder extends TxIndexBuilder {
    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
        Configuration conf = env.getConfiguration();
        // Install failure policy that just re-throws exception instead of killing RS
        // or disabling the index
        conf.set(IndexWriter.INDEX_FAILURE_POLICY_CONF_KEY, PhoenixTxIndexFailurePolicy.class.getName());
    }

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        // The entire purpose of this method impl is to get the existing rows for the
        // table rows being indexed into the block cache, as the index maintenance code
        // does a point scan per row.
        // TODO: provide a means for the transactional case to just return the Scanner
        // for when this is executed as it seems like that would be more efficient.
        IndexUtil.loadMutatingRowsIntoBlockCache(this.env.getRegion(), getCodec(), miniBatchOp, useRawScanToPrimeBlockCache());
    }

    private PhoenixIndexCodec getCodec() {
        return (PhoenixIndexCodec)this.codec;
    }
}
