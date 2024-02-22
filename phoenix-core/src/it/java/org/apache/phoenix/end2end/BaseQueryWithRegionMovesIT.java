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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.BeforeParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLTimeoutException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

/**
 * Basic tests that include region move hooks while scanning the rows.
 * <p>
 */
@RunWith(Parameterized.class)
public abstract class BaseQueryWithRegionMovesIT extends ParallelStatsDisabledWithRegionMovesIT {
    protected static final String tenantId = getOrganizationId();
    protected static final String NO_INDEX = "";
    protected static final String[] GLOBAL_INDEX_DDLS =
            new String[]{
                    "CREATE INDEX IF NOT EXISTS %s ON %s (a_integer DESC) INCLUDE (" +
                            "    A_STRING, "
                            + "    B_STRING, " + "    A_DATE)",
                    "CREATE INDEX IF NOT EXISTS %s ON %s (a_integer, a_string) INCLUDE (" +
                            "    B_STRING, "
                            + "    A_DATE)",
                    "CREATE INDEX IF NOT EXISTS %s ON %s (a_integer) INCLUDE (" + "    A_STRING, "
                            + "    B_STRING, " + "    A_DATE)",
                    NO_INDEX};
    private static final String[] INDEX_DDLS;

    static {
        INDEX_DDLS = new String[GLOBAL_INDEX_DDLS.length];
        int i = 0;
        for (String s : GLOBAL_INDEX_DDLS) {
            INDEX_DDLS[i++] = s;
        }
    }

    protected static Date date;
    protected static String tableName;
    protected static String indexName;

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseQueryWithRegionMovesIT.class);

    @BeforeParam
    public static synchronized void initTables(String idxDdl, boolean columnEncoded,
                                               boolean keepDeletedCells) throws Exception {
        StringBuilder optionBuilder = new StringBuilder();
        if (!columnEncoded) {
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (keepDeletedCells) {
            if (optionBuilder.length() > 0) optionBuilder.append(",");
            optionBuilder.append("KEEP_DELETED_CELLS=true");
        }
        String tableDDLOptions = optionBuilder.toString();
        try {
            tableName = initATableValues(generateUniqueName(), tenantId,
                    getDefaultSplits(tenantId),
                    date = new Date(System.currentTimeMillis()), null, getUrl(),
                    tableDDLOptions);
        } catch (Exception e) {
            LOGGER.error("Exception when creating aTable ", e);
            throw e;
        }
        indexName = generateUniqueName();
        if (idxDdl.length() > 0) {
            String indexDDL =
                    String.format(idxDdl, indexName, tableName);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty("phoenix.query.timeoutMs", "30000");
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                conn.createStatement().execute(indexDDL);
            } catch (SQLTimeoutException e) {
                LOGGER.info("Query timed out. Retrying one more time.", e);
                try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                    conn.createStatement().execute(indexDDL);
                } catch (Exception ex) {
                    LOGGER.error("Exception while creating index during second retry: "
                            + indexDDL, ex);
                    throw ex;
                }
            } catch (Exception e) {
                LOGGER.error("Exception while creating index: " + indexDDL, e);
                throw e;
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        hasTestStarted = true;
    }

    @After
    public void tearDown() throws Exception {
        countOfDummyResults = 0;
        TABLE_NAMES.clear();
        hasTestStarted = false;
    }

    public BaseQueryWithRegionMovesIT(String idxDdl, boolean columnEncoded,
                                      boolean keepDeletedCells) {
    }

    public static Collection<Object> allIndexes() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{false}) {
                testCases.add(new Object[]{indexDDL, columnEncoded, false});
            }
        }
        return testCases;
    }

    public static Collection<Object> allIndexesWithEncoded() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{false, true}) {
                testCases.add(new Object[]{indexDDL, columnEncoded, false});
            }
        }
        return testCases;
    }

    public static Collection<Object> allIndexesWithEncodedAndKeepDeleted() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{false, true}) {
                testCases.add(new Object[]{indexDDL, columnEncoded, true});
            }
        }
        return testCases;
    }
}
