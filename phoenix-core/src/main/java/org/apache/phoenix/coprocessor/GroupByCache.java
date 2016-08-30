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

import java.io.Closeable;

import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * 
 * Interface to abstract the way in which distinct group by
 * elements are cached
 *
 * 
 * @since 3.0.0
 */
public interface GroupByCache extends Closeable {
    long size();
    Aggregator[] cache(ImmutableBytesPtr key);
    RegionScanner getScanner(RegionScanner s);
}
