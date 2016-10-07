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

import org.apache.phoenix.query.BaseTest;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;

public class ParallelRunListener extends RunListener {
    // This causes output to go to the console when run through maven
    // private static final Log LOG = LogFactory.getLog(ParallelRunListener.class);
    private static final int TEAR_DOWN_THRESHOLD = 100;
    
    private int testRuns = 0;

    @Override
    public void testRunFinished(Result result) throws Exception {
        testRuns += result.getRunCount();
        if (testRuns > TEAR_DOWN_THRESHOLD) {
            // LOG.info("Tearing down mini cluster after " + testRuns + " test runs");
            testRuns = 0;
            BaseTest.tearDownMiniCluster();
        }

        super.testRunFinished(result);
    }
}
