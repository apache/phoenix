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
package org.apache.phoenix.end2end.index;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

/*
 * This class is to ensure gets its own cluster with Namespace Enabled
 */
public class MutableIndexFailureWithNamespaceIT extends MutableIndexFailureIT {

    public MutableIndexFailureWithNamespaceIT(boolean transactional, boolean localIndex, boolean isNamespaceMapped,
            Boolean disableIndexOnWriteFailure, boolean failRebuildTask, Boolean throwIndexWriteFailure) {
        super(transactional, localIndex, isNamespaceMapped, disableIndexOnWriteFailure, failRebuildTask,
                throwIndexWriteFailure);
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = getServerProps();
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        clientProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        NUM_SLAVES_BASE = 4;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
        TableName systemTable = SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                true);
        indexRebuildTaskRegionEnvironment = getUtility()
                .getRSForFirstRegionInTable(systemTable).getRegions(systemTable).get(0).getCoprocessorHost()
                .findCoprocessorEnvironment(MetaDataRegionObserver.class.getName());
        MetaDataRegionObserver.initRebuildIndexConnectionProps(indexRebuildTaskRegionEnvironment.getConfiguration());
    }
    
    @Parameters(name = "MutableIndexFailureIT_transactional={0},localIndex={1},isNamespaceMapped={2},disableIndexOnWriteFailure={3},failRebuildTask={4},throwIndexWriteFailure={5}") // name is used by failsafe as file name in reports
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][] { 
                // note - can't disableIndexOnWriteFailure without throwIndexWriteFailure, PHOENIX-4130
                { false, false, true, true, false, null},
                { false, false, true, true, false, true},
                { true, false, true, true, false, null},
                { false, true, true, true, false, null},
                { true, true, true, null, false, null},
                { false, false, true, true, true, null},
                { false, false, true, false, true, false},
                } 
        );
    }

}
