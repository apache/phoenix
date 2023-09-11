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
package org.apache.phoenix.end2end.index;

import java.util.Map;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

//FIXME shouldn't this be @NeedsOwnMiniClusterTest ?
@Category(ParallelStatsDisabledTest.class)
public class GlobalMutableNonTxIndexWithLazyPostBatchWriteIT extends GlobalMutableNonTxIndexIT {

    public GlobalMutableNonTxIndexWithLazyPostBatchWriteIT(boolean columnEncoded) {
        super(columnEncoded);
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(IndexRegionObserver.INDEX_LAZY_POST_BATCH_WRITE, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
}
