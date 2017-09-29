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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;



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
    protected static final String NO_INDEX = "";
    protected static final String[] GLOBAL_INDEX_DDLS =
            new String[] {
                    "CREATE INDEX %s ON %s (a_integer DESC) INCLUDE (" + "    A_STRING, "
                            + "    B_STRING, " + "    A_DATE) %s",
                    "CREATE INDEX %s ON %s (a_integer, a_string) INCLUDE (" + "    B_STRING, "
                            + "    A_DATE) %s",
                    "CREATE INDEX %s ON %s (a_integer) INCLUDE (" + "    A_STRING, "
                            + "    B_STRING, " + "    A_DATE) %s",
                    NO_INDEX };
    protected static final String[] LOCAL_INDEX_DDLS =
            new String[] {
                    "CREATE LOCAL INDEX %s ON %s (a_integer DESC) INCLUDE (" + "    A_STRING, "
                            + "    B_STRING, " + "    A_DATE) %s",
                    "CREATE LOCAL INDEX %s ON %s (a_integer, a_string) INCLUDE (" + "    B_STRING, "
                            + "    A_DATE) %s",
                    "CREATE LOCAL INDEX %s ON %s (a_integer) INCLUDE (" + "    A_STRING, "
                            + "    B_STRING, " + "    A_DATE) %s" };
    protected static String[] INDEX_DDLS;
    static {
        INDEX_DDLS = new String[GLOBAL_INDEX_DDLS.length + LOCAL_INDEX_DDLS.length];
        int i = 0;
        for (String s : GLOBAL_INDEX_DDLS) {
            INDEX_DDLS[i++] = s;
        }
        for (String s : LOCAL_INDEX_DDLS) {
            INDEX_DDLS[i++] = s;
        }
    }
    protected Date date;
    private String indexDDL;
    private String tableDDLOptions;
    protected String tableName;
    protected String indexName;

    private static final Logger logger = LoggerFactory.getLogger(BaseQueryIT.class);

    public BaseQueryIT(String idxDdl, boolean columnEncoded, boolean keepDeletedCells) throws Exception {
        StringBuilder optionBuilder = new StringBuilder();
        if (!columnEncoded) {
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (keepDeletedCells) {
            if (optionBuilder.length() > 0) optionBuilder.append(",");
            optionBuilder.append("KEEP_DELETED_CELLS=true");
        }
        this.tableDDLOptions = optionBuilder.toString();
        try {
            this.tableName =
                    initATableValues(generateUniqueName(), tenantId, getDefaultSplits(tenantId),
                        date = new Date(System.currentTimeMillis()), null, getUrl(),
                        tableDDLOptions);
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
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                conn.createStatement().execute(this.indexDDL);
            } catch (Exception e) {
                logger.error("Exception while creating index: " + indexDDL, e);
                throw e;
            }
        }
    }

    public static Collection<Object> allIndexes() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{false}) {
                testCases.add(new Object[] { indexDDL, columnEncoded });
            }
        }
        return testCases;
    }
}
