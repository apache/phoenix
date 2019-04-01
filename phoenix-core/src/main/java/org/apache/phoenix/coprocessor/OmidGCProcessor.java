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
package org.apache.phoenix.coprocessor;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.transaction.OmidCompactor;
import org.apache.omid.transaction.OmidSnapshotFilter;

import java.io.IOException;
import java.util.Optional;


public class OmidGCProcessor extends DelegateRegionObserver implements RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    public OmidGCProcessor() {
        super(new OmidCompactor(true));
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (delegate instanceof RegionCoprocessor) {
            ((RegionCoprocessor)delegate).start(env);
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        if (delegate instanceof RegionCoprocessor) {
            ((RegionCoprocessor)delegate).stop(env);
        }
    }

}
