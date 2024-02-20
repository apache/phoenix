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

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@Category(NeedsOwnMiniClusterTest.class)
public class GlobalImmutableTxIndexWithRegionMovesIT extends BaseIndexWithRegionMovesIT {

    public GlobalImmutableTxIndexWithRegionMovesIT(String transactionProvider,
                                                   boolean columnEncoded) {
        super(false, false, false, transactionProvider, columnEncoded);
    }

    @Parameters(name = "GlobalImmutableTxIndexWithRegionMovesIT_"
            + "transactionProvider={0},columnEncoded={1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // OMID does not support local indexes or column encoding
                {"OMID", false},
        });
    }

}
