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
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.hbase.index.covered.IndexCodec;

public abstract class BaseIndexCodec implements IndexCodec {
    /**
     * {@inheritDoc}
     * <p>
     * By default, the codec is always enabled. Subclasses should override this method if they want do
     * decide to index on a per-mutation basis.
     */
    @Override
    public boolean isEnabled(Mutation m) {
        return true;
    }
}