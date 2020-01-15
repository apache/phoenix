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
package org.apache.phoenix.mapreduce.index;

import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;

public class IndexScrutinyMapperForTest extends IndexScrutinyMapper {

    public static final int TEST_TABLE_TTL = 3600;
    public static class ScrutinyTestClock extends EnvironmentEdge {
        long initialTime;
        long delta;

        public ScrutinyTestClock(long delta) {
            initialTime = System.currentTimeMillis() + delta;
            this.delta = delta;
        }

        @Override
        public long currentTime() {
            return System.currentTimeMillis() + delta;
        }
    }

    @Override
    public void preQueryTargetTable() {
        // change the current time past ttl
        ScrutinyTestClock clock = new ScrutinyTestClock(TEST_TABLE_TTL*1000);
        EnvironmentEdgeManager.injectEdge(clock);
    }
}
