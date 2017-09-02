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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;



/**
 * 
 * Basic tests for Phoenix JDBC implementation
 *
 * 
 * @since 0.1
 */

@RunWith(Parameterized.class)
public abstract class BaseQueryIT extends ParallelStatsDisabledIT {
    protected static final String tenantId = getOrganizationId();
    protected static final String ATABLE_INDEX_NAME = "ATABLE_IDX";
    protected static final long BATCH_SIZE = 3;
    protected static final String[] INDEX_DDLS = new String[] {
            "CREATE INDEX %s ON %s (a_integer DESC) INCLUDE ("
                    + "    A_STRING, " + "    B_STRING, " + "    A_DATE) %s",
            "CREATE INDEX %s ON %s (a_integer, a_string) INCLUDE ("
                    + "    B_STRING, " + "    A_DATE) %s",
            "CREATE INDEX %s ON %s (a_integer) INCLUDE ("
                    + "    A_STRING, " + "    B_STRING, " + "    A_DATE) %s",
            "CREATE LOCAL INDEX %s ON %s (a_integer DESC) INCLUDE ("
                    + "    A_STRING, " + "    B_STRING, " + "    A_DATE) %s",
            "CREATE LOCAL INDEX %s ON %s (a_integer, a_string) INCLUDE (" + "    B_STRING, "
                    + "    A_DATE) %s",
            "CREATE LOCAL INDEX %s ON %s (a_integer) INCLUDE ("
                    + "    A_STRING, " + "    B_STRING, " + "    A_DATE) %s",
            "" };

    @BeforeClass
    @Shadower(classBeingShadowed = ParallelStatsDisabledIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        // Make a small batch size to test multiple calls to reserve sequences
        props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB, Long.toString(BATCH_SIZE));
        // Must update config before starting server
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    protected long ts;
    protected Date date;
    private String indexDDL;
    private String tableDDLOptions;
    protected String tableName;
    protected String indexName;
    
    private static final Logger logger = LoggerFactory.getLogger(BaseQueryIT.class);

    public BaseQueryIT(String idxDdl, boolean mutable, boolean columnEncoded,
            boolean keepDeletedCells) throws Exception {
        StringBuilder optionBuilder = new StringBuilder();
        if (!columnEncoded) {
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (!mutable) {
            if (optionBuilder.length()>0)
                optionBuilder.append(",");
            optionBuilder.append("IMMUTABLE_ROWS=true");
            if (!columnEncoded) {
                optionBuilder.append(",IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            }
        }
        if (keepDeletedCells) {
            if (optionBuilder.length()>0)
                optionBuilder.append(",");
            optionBuilder.append("KEEP_DELETED_CELLS=true");
        }
        this.tableDDLOptions = optionBuilder.toString();
        this.ts = nextTimestamp();
        try {
            this.tableName =
                    initATableValues(generateUniqueName(), tenantId, getDefaultSplits(tenantId),
                        date = new Date(System.currentTimeMillis()), ts, getUrl(), tableDDLOptions);
        } catch (Exception e) {
            logger.error("Exception when creating aTable ", e);
            throw e;
        }
        this.indexName = generateUniqueName();
        if (idxDdl.length() > 0) {
            this.indexDDL =
                    String.format(idxDdl, indexName, tableName,
                        keepDeletedCells ? "KEEP_DELETED_CELLS=true" : "KEEP_DELETED_CELLS=false");
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));

            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                conn.createStatement().execute(this.indexDDL);
            } catch (Exception e) {
                logger.error("Exception while creating index: " + indexDDL, e);
                throw e;
            }
        }
    }
    
    @Parameters(name="indexDDL={0},mutable={1},columnEncoded={2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean mutable : new boolean[]{false}) {
                for (boolean columnEncoded : new boolean[]{false}) {
                    testCases.add(new Object[] { indexDDL, mutable, columnEncoded });
                }
            }
        }
        return testCases;
    }
    
    protected static boolean compare(CompareOp op, ImmutableBytesWritable lhsOutPtr, ImmutableBytesWritable rhsOutPtr) {
        int compareResult = Bytes.compareTo(lhsOutPtr.get(), lhsOutPtr.getOffset(), lhsOutPtr.getLength(), rhsOutPtr.get(), rhsOutPtr.getOffset(), rhsOutPtr.getLength());
        return ByteUtil.compare(op, compareResult);
    }

    protected static void analyzeTable(Connection conn, String tableName) throws IOException, SQLException {
        String query = "UPDATE STATISTICS " + tableName;
        conn.createStatement().execute(query);
    }
    
    private static AtomicInteger runCount = new AtomicInteger(0);
    protected static int nextRunCount() {
        return runCount.getAndAdd(1);
    }

}
