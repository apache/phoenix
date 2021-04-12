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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.hbase.index.util.VersionUtil;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

/**
 * 
 * Conditionally run tests based on HBase version. Uses a
 * @minVersion(versionStr) annotation on either the test class
 * or the test method.
 *
 */
public class MinVersionTestRunner extends BlockJUnit4ClassRunner {
    
    public MinVersionTestRunner(Class klass) throws InitializationError {
        super(klass);
    }
    @Override
    public void runChild(FrameworkMethod method, RunNotifier notifier) {
        MinVersion methodCondition = method.getAnnotation(MinVersion.class);
        MinVersion classCondition = this.getTestClass().getJavaClass().getAnnotation(MinVersion.class);
        String versionStr = VersionInfo.getVersion();
        int version = VersionUtil.encodeVersion(versionStr);
        if (  (methodCondition == null || version >= VersionUtil.encodeVersion(methodCondition.value()))
           && (classCondition == null || version >= VersionUtil.encodeVersion(classCondition.value()))) {
            super.runChild(method, notifier);
        } else {
            notifier.fireTestIgnored(describeChild(method));
        }
    }
    
    @Target( {ElementType.TYPE, ElementType.METHOD} )
    @Retention(RetentionPolicy.RUNTIME)
    public @interface MinVersion {
        /** The minimum version supported for this test class or test method */
        String value();
    }}

