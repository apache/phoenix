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
import org.junit.AfterClass;
import org.junit.experimental.categories.Category;

/**
 * Base class for tests that need to be executed on their mini cluster. You must create unique names using
 * {@link #generateUniqueName()} for each table and sequence used to prevent collisions.
 * <p>
 * TODO: Convert all tests extending {@link BaseOwnClusterIT} to use unique names for tables and sequences. Once that is
 * done either rename this class or get rid of the {@link BaseOwnClusterIT} base class.
 * </p>
 */
@Category(NeedsOwnMiniClusterTest.class)
public class BaseUniqueNamesOwnClusterIT extends BaseTest {
    @AfterClass
    public static void doTeardown() throws Exception {
        tearDownMiniCluster();
    }
}
