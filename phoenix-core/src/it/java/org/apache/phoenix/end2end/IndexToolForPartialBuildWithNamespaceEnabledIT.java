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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

/**
 * Tests for the {@link IndexToolForPartialBuildWithNamespaceEnabled}
 */
@RunWith(Parameterized.class)
public class IndexToolForPartialBuildWithNamespaceEnabledIT extends IndexToolForPartialBuildIT {
    
    
    public IndexToolForPartialBuildWithNamespaceEnabledIT(boolean localIndex, boolean isNamespaceEnabled) {
        super(localIndex);
        this.isNamespaceEnabled=isNamespaceEnabled;
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(7);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
        serverProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
        serverProps.put("hbase.client.pause", "5000");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_PERIOD, "2000");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, "1000");
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    @Parameters(name="localIndex = {0} , isNamespaceEnabled = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, true},{ true, false }
           });
    }
    
}
