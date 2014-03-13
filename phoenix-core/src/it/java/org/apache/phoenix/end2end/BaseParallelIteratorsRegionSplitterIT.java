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

import static org.apache.phoenix.util.TestUtil.STABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;


public class BaseParallelIteratorsRegionSplitterIT extends BaseClientManagedTimeIT {

    protected static final byte[] KMIN  = new byte[] {'!'};
    protected static final byte[] KMIN2  = new byte[] {'.'};
    protected static final byte[] K1  = new byte[] {'a'};
    protected static final byte[] K3  = new byte[] {'c'};
    protected static final byte[] K4  = new byte[] {'d'};
    protected static final byte[] K5  = new byte[] {'e'};
    protected static final byte[] K6  = new byte[] {'f'};
    protected static final byte[] K9  = new byte[] {'i'};
    protected static final byte[] K11 = new byte[] {'k'};
    protected static final byte[] K12 = new byte[] {'l'};
    protected static final byte[] KMAX  = new byte[] {'~'};
    protected static final byte[] KMAX2  = new byte[] {'z'};
    
    @BeforeClass
    public static void doSetup() throws Exception {
        int targetQueryConcurrency = 3;
        int maxQueryConcurrency = 5;
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB, Integer.toString(maxQueryConcurrency));
        props.put(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB, Integer.toString(targetQueryConcurrency));
        props.put(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, Integer.toString(Integer.MAX_VALUE));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    protected void initTableValues(long ts) throws Exception {
        byte[][] splits = new byte[][] {K3,K4,K9,K11};
        ensureTableCreated(getUrl(),STABLE_NAME,splits, ts-2);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + STABLE_NAME + " VALUES (?, ?)");
        stmt.setString(1, new String(KMIN));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String(KMAX));
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();
        conn.close();
    }

    protected static TableRef getTableRef(Connection conn, long ts) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        TableRef table = new TableRef(null,pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), STABLE_NAME)),ts, false);
        return table;
    }
    
}
