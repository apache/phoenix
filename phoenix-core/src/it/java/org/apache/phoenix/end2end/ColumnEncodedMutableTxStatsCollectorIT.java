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
import org.junit.runners.Parameterized.Parameters;

public class ColumnEncodedMutableTxStatsCollectorIT extends StatsCollectorIT {

    public ColumnEncodedMutableTxStatsCollectorIT(boolean mutable, boolean transactional, String transactionProvider,
            boolean userTableNamespaceMapped, boolean columnEncoded) {
        super(mutable, transactional, transactionProvider, userTableNamespaceMapped, columnEncoded);
    }

    @Parameters(name = "mutable={0},transactional={1},transactionProvider={2},isUserTableNamespaceMapped={3},columnEncoded={4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
            new Object[][] { { true, true, "TEPHRA", false, true }, { true, true, "TEPHRA", true, true } });
    }
}
