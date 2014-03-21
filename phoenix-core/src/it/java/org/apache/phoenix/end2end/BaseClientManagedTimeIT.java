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

import org.junit.Before;

/**
 * 
 * Base class for tests that manage their own time stamps
 * We need to separate these from tests that manged the time stamp
 * themselves, because we create/destroy the Phoenix tables
 * between tests and only allow a table time stamp to increase.
 * If we let HBase set the time stamps, then our client time stamps
 * will usually be smaller than these time stamps and the table
 * deletion/creation would fail.
 * 
 * 
 * @since 0.1
 */
public abstract class BaseClientManagedTimeIT extends BaseConnectedQueryIT {
    @Before
    public void doTestSetup() throws Exception {
        long ts = nextTimestamp();
        deletePriorTables(ts-1);    
    }
}
