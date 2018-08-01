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
package org.apache.phoenix.hbase.index.covered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.builder.BaseIndexCodec;

/**
 * An {@link IndexCodec} for testing that allow you to specify the index updates/deletes, regardless of the current
 * tables' state.
 */
public class CoveredIndexCodecForTesting extends BaseIndexCodec {

    private List<IndexUpdate> deletes = new ArrayList<IndexUpdate>();
    private List<IndexUpdate> updates = new ArrayList<IndexUpdate>();

    public void addIndexDelete(IndexUpdate... deletes) {
        this.deletes.addAll(Arrays.asList(deletes));
    }

    public void addIndexUpserts(IndexUpdate... updates) {
        this.updates.addAll(Arrays.asList(updates));
    }

    public void clear() {
        this.deletes.clear();
        this.updates.clear();
    }

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state, IndexMetaData context) {
        return this.deletes;
    }

    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state, IndexMetaData context) {
        return this.updates;
    }

    @Override
    public void initialize(Configuration conf, byte[] regionStartKey, byte[] regionEndKey, byte[] tableName) {
        // noop
    }

    @Override
    public boolean isEnabled(Mutation m) {
        return true;
    }
}