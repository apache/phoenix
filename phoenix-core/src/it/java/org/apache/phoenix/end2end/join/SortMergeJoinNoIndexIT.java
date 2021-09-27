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
package org.apache.phoenix.end2end.join;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(ParallelStatsDisabledTest.class)
public class SortMergeJoinNoIndexIT extends SortMergeJoinIT {

    private static final Map<String,String> virtualNameToRealNameMap = Maps.newHashMap();
    private static final String schemaName = "S_" + generateUniqueName();

    @Override
    protected String getSchemaName() {
        // run all tests in a single schema
        return schemaName;
    }

    @Override
    protected Map<String,String> getTableNameMap() {
        // cache across tests, so that tables and
        // indexes are not recreated each time
        return virtualNameToRealNameMap;
    }

    public SortMergeJoinNoIndexIT(String[] indexDDL, String[] plans) {
        super(indexDDL, plans);
    }

    @Parameters(name="SortMergeJoinNoIndexIT_{index}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                {}, {
                "SORT-MERGE-JOIN (LEFT) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_SUPPLIER_TABLE_FULL_NAME + "\n" +
                "AND\n" +
                "    SORT-MERGE-JOIN (INNER) TABLES\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "    AND (SKIP MERGE)\n" +
                "        CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "            SERVER FILTER BY QUANTITY < 5000\n" +
                "            SERVER SORTED BY [\"O.item_id\"]\n" +
                "        CLIENT MERGE SORT\n" +
                "    CLIENT SORTED BY [\"I.supplier_id\"]",
                
                "SORT-MERGE-JOIN (INNER) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ORDER_TABLE_FULL_NAME + "\n" +
                "        SERVER SORTED BY [\"O.item_id\"]\n" +
                "    CLIENT MERGE SORT\n" +
                "CLIENT 4 ROW LIMIT",
                
                "SORT-MERGE-JOIN (INNER) TABLES\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "AND\n" +
                "    CLIENT PARALLEL 1-WAY FULL SCAN OVER " + JOIN_ITEM_TABLE_FULL_NAME + "\n" +
                "        SERVER FILTER BY FIRST KEY ONLY"
                }});
        return testCases;
    }
}
