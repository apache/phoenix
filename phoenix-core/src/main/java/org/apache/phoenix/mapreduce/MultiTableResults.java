/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Keys and values for results across tables.
 */
public class MultiTableResults {
    private ImmutableBytesWritable sourceKey;
    private Map<String, Object> sourceResults;
    private ImmutableBytesWritable targetKey;
    private Map<String, Object> targetResults;

    public MultiTableResults(ImmutableBytesWritable sourceKey, Map<String, Object> sourceResults,
            ImmutableBytesWritable targetKey, Map<String, Object> targetResults) {
        this.sourceKey = sourceKey;
        this.sourceResults = sourceResults;
        this.targetKey = targetKey;
        this.targetResults = targetResults;
    }

    public ImmutableBytesWritable getSourceKey() {
        return sourceKey;
    }

    public Map<String, Object> getSourceResults() {
        return sourceResults;
    }

    public ImmutableBytesWritable getTargetKey() {
        return targetKey;
    }

    public Map<String, Object> getTargetResults() {
        return targetResults;
    }
}
