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

import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;

public class GlobalImmutableNonTxIndexIT extends BaseIndexIT {

    public GlobalImmutableNonTxIndexIT(boolean localIndex, boolean mutable, String transactionProvider, boolean columnEncoded) {
        super(localIndex, mutable, transactionProvider, columnEncoded);
    }

    @Parameters(name="GlobalImmutableNonTxIndexIT_localIndex={0},mutable={1},transactionProvider={2},columnEncoded={3}") // name is used by failsafe as file name in reports
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { false, false, null, false }, { false, false, null, true }
           });
    }

}
