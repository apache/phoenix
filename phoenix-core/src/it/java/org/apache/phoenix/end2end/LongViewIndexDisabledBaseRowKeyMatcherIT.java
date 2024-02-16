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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class LongViewIndexDisabledBaseRowKeyMatcherIT extends BaseRowKeyMatcherTestIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        conf.set(QueryServices.PHOENIX_TABLE_TTL_ENABLED, String.valueOf(true));
        conf.set(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(false));
        conf.set(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, "true");
        conf.set(IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY,
                "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");

        // Clear the cached singletons so we can inject our own.
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });

        Map<String, String> DEFAULT_PROPERTIES = new HashMap() ;
        setUpTestDriver(new ReadOnlyProps(DEFAULT_PROPERTIES.entrySet().iterator()));
    }

    @Override
    protected boolean hasLongViewIndexEnabled() {
        return false;
    }
}
