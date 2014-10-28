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

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

/**
 * Base class for tests that manage their own time stamps
 * We need to separate these from tests that rely on hbase to set
 * timestamps, because we create/destroy the Phoenix tables
 * between tests and only allow a table time stamp to increase.
 * Without this separation table deletion/creation would fail.
 * 
 * All tests extending this class use the mini cluster that is
 * different from the mini cluster used by test classes extending 
 * {@link BaseHBaseManagedTimeIT}.
 * 
 * @since 0.1
 */
@NotThreadSafe
@Category(ClientManagedTimeTest.class)
public abstract class BaseClientManagedTimeIT extends BaseTest {
    protected static Configuration getTestClusterConfig() {
        // don't want callers to modify config.
        return new Configuration(config);
    }
    
    @After
    public void cleanUpAfterTest() throws Exception {
        long ts = nextTimestamp();
        deletePriorTables(ts - 1, getUrl());    
    }
    
    public static Map<String,String> getDefaultProps() {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        // Must update config before starting server
        props.put(QueryServices.STATS_USE_CURRENT_TIME_ATTRIB, Boolean.FALSE.toString());
        return props;
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = getDefaultProps();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @AfterClass
    public static void doTeardown() throws Exception {
        dropNonSystemTables();
    }
}
