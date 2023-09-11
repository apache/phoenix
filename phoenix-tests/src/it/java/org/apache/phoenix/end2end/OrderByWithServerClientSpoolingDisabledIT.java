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
package org.apache.phoenix.end2end;

import java.util.Map;

import org.apache.phoenix.iterate.SizeBoundQueue;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Same as the order by test but with spooling disabled both on the server and client. This will use
 * {@link SizeBoundQueue} for all its operations
 */
@Category(NeedsOwnMiniClusterTest.class)
public class OrderByWithServerClientSpoolingDisabledIT extends BaseOrderByIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // make sure disabling server side spooling has no affect on correctness(existing orderby
        // IT)
        props.put(QueryServices.SERVER_ORDERBY_SPOOLING_ENABLED_ATTRIB,
            Boolean.toString(Boolean.FALSE));
        props.put(QueryServices.CLIENT_ORDERBY_SPOOLING_ENABLED_ATTRIB,
            Boolean.toString(Boolean.FALSE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

}
