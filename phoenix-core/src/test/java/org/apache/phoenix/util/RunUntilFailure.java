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
package org.apache.phoenix.util;

import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class RunUntilFailure extends BlockJUnit4ClassRunner {  
    private boolean hasFailure;

    public RunUntilFailure(Class<?> klass) throws InitializationError {  
        super(klass);  
    }  

    @Override  
    protected Description describeChild(FrameworkMethod method) {  
        if (method.getAnnotation(Repeat.class) != null &&  
                method.getAnnotation(Ignore.class) == null) {  
            return describeRepeatTest(method);  
        }  
        return super.describeChild(method);  
    }  

    private Description describeRepeatTest(FrameworkMethod method) {  
        int times = method.getAnnotation(Repeat.class).value();  

        Description description = Description.createSuiteDescription(  
                testName(method) + " [" + times + " times]",  
                method.getAnnotations());  

        for (int i = 1; i <= times; i++) {  
            description.addChild(Description.createTestDescription(  
                    getTestClass().getJavaClass(),  
                    testName(method) + "-" + i));  
        }  
        return description;  
    }  

    @Override  
    protected void runChild(final FrameworkMethod method, RunNotifier notifier) {  
        Description description = describeChild(method);  

        if (method.getAnnotation(Repeat.class) != null &&  
                method.getAnnotation(Ignore.class) == null) {  
            runRepeatedly(methodBlock(method), description, notifier);  
        }  
        super.runChild(method, notifier);  
    }  

    private void runRepeatedly(Statement statement, Description description,  
            RunNotifier notifier) {
        notifier.addListener(new RunListener() {
            @Override
            public void testFailure(Failure failure) {
                hasFailure = true;
            }
        });
        for (Description desc : description.getChildren()) {  
            if (hasFailure) {
                notifier.fireTestIgnored(desc);
            } else if(!desc.isSuite()) {
                runLeaf(statement, desc, notifier);
            }
        }  
    }  

}  

