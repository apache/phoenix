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
package org.apache.phoenix.trace;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * Test runner to run classes that depend on Hadoop1 compatibility that may not be present for the
 * feature
 */
public class Hadoop1TracingTestEnabler extends BlockJUnit4ClassRunner {

    public Hadoop1TracingTestEnabler(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    public void runChild(FrameworkMethod method, RunNotifier notifier) {
        // if the class is already disabled, then we can disable on the class level, otherwise we
        // just check the per-method
        Hadoop1Disabled condition =
                getTestClass().getJavaClass().getAnnotation(Hadoop1Disabled.class);
        if (condition == null) {
            condition = method
                        .getAnnotation(Hadoop1Disabled.class);
        }

        // if this has the flag, then we want to disable it if hadoop1 is not enabled for that
        // feature
        if (condition != null && getEnabled(condition.value())) {
            super.runChild(method, notifier);
        } else {
            notifier.fireTestIgnored(describeChild(method));
        }
    }

    /**
     * Simple check that just uses if-else logic. We can move to something more complex, policy
     * based later when this gets more complex.
     * @param feature name of the feature to check
     * @return <tt>true</tt> if the test method is enabled for the given feature, <tt>false</tt>
     *         otherwise
     */
    private boolean getEnabled(String feature) {
        if (feature.equals("tracing")) {
            return !BaseTracingTestIT.shouldEarlyExitForHadoop1Test();
        }
        return true;
    }

    /**
     * Marker that a class/method should be disabled if hadoop1 features are not enabled. It takes a
     * value for the Hadoop1 feature on which this class/method depends, for instance "tracing" is
     * not supported in Hadoop1 (yet).
     */
    @Target({ ElementType.TYPE, ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface Hadoop1Disabled {
        String value();
    }
}
