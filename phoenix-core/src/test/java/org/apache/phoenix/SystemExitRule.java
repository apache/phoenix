/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test rule that prevents System.exit / JVM exit to error out the test runner, which manages
 * JVM and providing test output files, and instead throw valid Exception to handle JVM exit
 * gracefully
 */
public class SystemExitRule implements TestRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemExitRule.class);
    private static final SecurityManager SECURITY_MANAGER = new TestSecurityManager();

    @Override
    public Statement apply(final Statement s, Description d) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    System.setSecurityManager(SECURITY_MANAGER);
                    s.evaluate();
                } catch (UnsupportedOperationException e) {
                    LOGGER.warn("Was unable to set SecurityManager, JVM exits in tests will not be"
                            + "handled correctly ", e);
                } finally {
                    try {
                        System.setSecurityManager(null);
                    } catch (UnsupportedOperationException e) {
                        //We have logged a warning above already
                    }
                }
            }

        };
    }

    // Exiting the JVM is not allowed in tests and this exception is thrown instead
    // when it is done
    public static class SystemExitInTestException extends SecurityException {
      // empty
    }

}
