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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Queue;

/**
 * Base class for tests that let HBase set timestamps.
 * We need to separate these from tests that rely on clients
 * to set timestamps, because we create/destroy the Phoenix tables
 * between tests and only allow a table time stamp to increase.
 * Without this separation table deletion/creation would fail.
 *
 * All tests extending this class use the mini cluster that is
 * shared by all classes extending this class
 *
 * Remember to use BaseTest.generateRandomString() to generate table
 * names for your tests otherwise there might be naming collisions between
 * other tests.
 * {@link BaseClientManagedTimeIT}.
 *
 * @since 0.1
 */
@NotThreadSafe
@Category(HBaseManagedTimeTableReuseTest.class)
public class BaseHBaseManagedTimeTableReuseIT extends BaseTest {
    /*protected static Configuration getTestClusterConfig() {
        // don't want callers to modify config.
        return new Configuration(config);
    }*/

    @BeforeClass
    public static void doSetup() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    }

    @AfterClass
    public static void doTeardown() throws Exception {
        // no teardown since we are creating unique table names
        // just destroy our test driver
        //destroyDriver();
        //Queue connectionQueue = driver.getConnectionQueue();
        //List<>
        /*
        for (Object c : connectionQueue){
            Connection conn = (Connection) c;
            PMetaData pMetaDataCache = conn.unwrap(PhoenixConnection.class).getMetaDataCache();
            Iterator metaDataCacheIterator = pMetaDataCache.iterator();
            while(metaDataCacheIterator.hasNext()) {
                PTable pTable = (PTable)metaDataCacheIterator.next();
                String pTableName = pTable.getPhysicalName().getString();
                String a = pTableName;

            }
        }*/
        dropNonSystemTables(false);
    }

    @After
    public void cleanUpAfterTest() throws Exception {
        // no cleanup since we are using unique table names
    }

}
