/*
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

package org.apache.phoenix.hive;

import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class HiveMapReduceIT extends HivePhoenixStoreIT {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        final String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir != null && hadoopConfDir.length() != 0) {
            fail("HADOOP_CONF_DIR is non-empty in the current shell environment which will very likely cause this test to fail.");
        }
        setup(HiveTestUtil.MiniClusterType.mr);
    }
    
    @Override
    @Test
    @Ignore 
    /**
     * Ignoring because precicate pushdown is skipped for MR (ref:HIVE-18873) when there are multiple aliases
     */
    public void testJoinNoColumnMaps() throws Exception {
        
    }
    
    @Override
    @Test
    @Ignore 
    /**
     * Ignoring because projection pushdown is incorrect for MR when there are multiple aliases (ref:HIVE-18872)
     */
    public void testJoinColumnMaps() throws Exception {
        
    }
}
