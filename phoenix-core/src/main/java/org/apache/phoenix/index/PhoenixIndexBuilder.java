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
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.write.IndexWriter;

/**
 * Index builder for covered-columns index that ties into phoenix for faster use.
 */
public class PhoenixIndexBuilder extends NonTxIndexBuilder {

    @Override
    public IndexMetaData getIndexMetaData(MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
        return new PhoenixIndexMetaData(env, miniBatchOp.getOperation(0).getAttributesMap());
    }

    protected PhoenixIndexCodec getCodec() {
        return (PhoenixIndexCodec)codec;
    }

    @Override
    public void setup(RegionCoprocessorEnvironment env) throws IOException {
        super.setup(env);
        Configuration conf = env.getConfiguration();
        // Install handler that will attempt to disable the index first before killing the region
        // server
        conf.setIfUnset(IndexWriter.INDEX_FAILURE_POLICY_CONF_KEY,
            PhoenixIndexFailurePolicy.class.getName());
    }

    @Override
    public void batchStarted(MiniBatchOperationInProgress<Mutation> miniBatchOp, IndexMetaData context) throws IOException {
    }
}