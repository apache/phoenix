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

package org.apache.phoenix.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.phoenix.schema.PTable;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.CDC_INCLUDE_SCOPES;

public class CDCUtil {
    public static final String CDC_INDEX_PREFIX = "__CDC__";
    public static final String CDC_INDEX_TYPE_LOCAL = "L";

    /**
     * Make a set of CDC change scope enums from the given string containing comma separated scope
     * names.
     *
     * @param includeScopes Comma-separated scope names.
     * @return the set of enums, which can be empty if the string is empty or has no valid names.
     */
    public static Set<PTable.CDCChangeScope> makeChangeScopeEnumsFromString(String includeScopes) {
        Set<PTable.CDCChangeScope> cdcChangeScopes = new HashSet<>();
        if (includeScopes != null) {
            StringTokenizer st  = new StringTokenizer(includeScopes, ",");
            while (st.hasMoreTokens()) {
                String tok = st.nextToken();
                try {
                    cdcChangeScopes.add(PTable.CDCChangeScope.valueOf(tok.trim().toUpperCase()));
                }
                catch (IllegalArgumentException e) {
                    // Just ignore unrecognized scopes.
                }
            }
        }
        return cdcChangeScopes;
    }

    /**
     * Make a string of comma-separated scope names from the specified set of enums.
     *
     * @param includeScopes Set of scope enums
     * @return the comma-separated string of scopes, which can be an empty string in case the set is empty.
     */
    public static String makeChangeScopeStringFromEnums(Set<PTable.CDCChangeScope> includeScopes) {
        String cdcChangeScopes = null;
        if (includeScopes != null) {
            Iterable<String> tmpStream = () -> includeScopes.stream().sorted()
                    .map(s -> s.name()).iterator();
            cdcChangeScopes = StringUtils.join(",", tmpStream);
        }
        return cdcChangeScopes;
    }

    public static String getCDCIndexName(String cdcName) {
        return CDC_INDEX_PREFIX + cdcName;
    }

    public static String getCDCNameFromIndexName(String indexName) {
        assert(indexName.startsWith(CDC_INDEX_PREFIX));
        return indexName.substring(CDC_INDEX_PREFIX.length());
    }

    public static boolean isACDCIndex(String indexName) {
        return indexName.startsWith(CDC_INDEX_PREFIX);
    }

    public static boolean isACDCIndex(PTable indexTable) {
        return isACDCIndex(indexTable.getTableName().getString());
    }
}
