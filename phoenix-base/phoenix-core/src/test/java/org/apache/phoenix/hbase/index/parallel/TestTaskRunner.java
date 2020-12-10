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
package org.apache.phoenix.hbase.index.parallel;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

public class TestTaskRunner {

    @Test
    public void testWaitForCompletionTaskRunner() throws Exception {
        TaskRunner tr = new WaitForCompletionTaskRunner(Executors.newFixedThreadPool(4));
        TaskBatch<Boolean> tasks = new TaskBatch<Boolean>(4);
        for (int i = 0; i < 4; i++) {
            tasks.add(new EvenNumberFailingTask(i));
        }
        Pair<List<Boolean>, List<Future<Boolean>>> resultAndFutures =
                tr.submitUninterruptible(tasks);
        List<Boolean> results = resultAndFutures.getFirst();
        List<Future<Boolean>> futures = resultAndFutures.getSecond();
        for (int j = 0; j < 4; j++) {
            if (j % 2 == 0) {
                assertNull(results.get(j));
                try {
                    futures.get(j).get();
                    fail("Should have received ExecutionException");
                } catch (Exception e) {
                    assertTrue(e instanceof ExecutionException);
                    assertTrue(e.getCause().getMessage().equals("Even number task"));
                }
            } else {
                assertTrue(results.get(j));
            }
        }
    }

    private static class EvenNumberFailingTask extends Task<Boolean> {
        private int num;

        public EvenNumberFailingTask(int i) {
            this.num = i;
        }

        @Override
        public Boolean call() throws Exception {
            if (num % 2 == 0) {
                throw new IOException("Even number task");
            }
            return Boolean.TRUE;
        }

    }

}
