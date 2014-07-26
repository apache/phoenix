/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.call;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Helper class to run a Call with a set of {@link CallWrapper}
 */
public class CallRunner {

    /**
     * Helper {@link Callable} that also declares the type of exception it will throw, to help with
     * type safety/generics for java
     * @param <V> value type returned
     * @param <E> type of check exception thrown
     */
    public static interface CallableThrowable<V, E extends Exception> extends Callable<V> {
        @Override
        public V call() throws E;
    }

    private static final Log LOG = LogFactory.getLog(CallRunner.class);

    private CallRunner() {
        // no ctor for util class
    }

    public static <V, E extends Exception, T extends CallableThrowable<V, E>> V run(T call,
            CallWrapper... wrappers) throws E {
        try {
            for (CallWrapper wrap : wrappers) {
                wrap.before();
            }
            return call.call();
        } finally {
            // have to go in reverse, to match the before logic
            for (int i = wrappers.length - 1; i >= 0; i--) {
                try {
                    wrappers[i].after();
                } catch (Exception e) {
                    LOG.error("Failed to complete wrapper " + wrappers[i], e);
                }
            }
        }
    }

}
