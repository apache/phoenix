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

import java.util.Arrays;
import java.util.Collection;

import org.apache.phoenix.schema.stats.StatsCollectorIT;
import org.apache.phoenix.util.TestUtil;
import org.junit.runners.Parameterized.Parameters;

public class NonColumnEncodedImmutableTxStatsCollectorIT extends StatsCollectorIT {

    public NonColumnEncodedImmutableTxStatsCollectorIT(boolean mutable, String transactionProvider,
            boolean userTableNamespaceMapped, boolean columnEncoded) {
        super(mutable,transactionProvider, userTableNamespaceMapped, columnEncoded);
    }

    @Parameters(name = "mutable={0},transactionProvider={1},isUserTableNamespaceMapped={2},columnEncoded={3}")
    public static Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(
            new Object[][] { 
                { false, "TEPHRA", false, false }, { false, "TEPHRA", true, false },
                { false, "OMID", false, false }, { false, "OMID", true, false },
            }),1);
    }
}
