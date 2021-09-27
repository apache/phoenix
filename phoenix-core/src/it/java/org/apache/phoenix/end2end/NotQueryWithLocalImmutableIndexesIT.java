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

import java.util.Collection;
import java.util.List;

import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(ParallelStatsDisabledTest.class)
public class NotQueryWithLocalImmutableIndexesIT extends NotQueryIT {

    public NotQueryWithLocalImmutableIndexesIT(String indexDDL, boolean columnEncoded, boolean keepDeleted) throws Exception {
        super(indexDDL, columnEncoded, keepDeleted);
    }

    @Parameters(name = "localIndexDDL={0}")
    public static synchronized Collection<Object> localIndexes() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : LOCAL_INDEX_DDLS) {
            testCases.add(new Object[] { indexDDL, false, false });
        }
        return testCases;
    }

}
