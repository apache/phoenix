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

import org.junit.After;
import org.junit.AfterClass;

/**
 * Base class for tests whose methods control time stamps at which SQL statements are executed You must create unique
 * names using {@link #generateUniqueName()} for each table and sequence used to prevent collisions. Because of
 * uniqueness of table names and sequences, classes extending this class can execute in parallel (on the same JVM).
 */
public abstract class ParallelClientManagedTimeIT extends BaseClientManagedTimeIT {
//    @Override
//    @After
//    public void cleanUpAfterTest() throws Exception {
//        // Don't do anything as tests use unique table names and sequences
//    }

    @AfterClass
    public static void doTeardown() throws Exception {
        // Don't do anything as tests use unique table names and sequences
    }
}
