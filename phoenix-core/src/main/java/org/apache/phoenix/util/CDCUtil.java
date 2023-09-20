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

import org.apache.hadoop.util.StringUtils;

import org.apache.phoenix.schema.PTable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CDCUtil {
    public static Set<PTable.CDCChangeScope> makeChangeScopeEnumsFromString(String includeScopes) {
        Set<PTable.CDCChangeScope> cdcChangeScopes = new HashSet<>();
        if (includeScopes != null) {
            String[] toks = includeScopes.split("\\s*,\\s*");
            for (String tok: toks) {
                try {
                    cdcChangeScopes.add(PTable.CDCChangeScope.valueOf(tok.toUpperCase()));
                }
                catch (IllegalArgumentException e) {
                    // Just ignore unrecognized scopes.
                }
            }
        }
        return cdcChangeScopes;
    }

    public static String makeChangeScopeStringFromEnums(Set<PTable.CDCChangeScope> includeScopes) {
        String cdcChangeScopes = "";
        if (includeScopes != null) {
            Iterable<String> tmpStream = () -> includeScopes.stream().sorted()
                    .map(s -> s.name()).iterator();
            cdcChangeScopes = StringUtils.join(", ", tmpStream);
        }
        return cdcChangeScopes;
    }
}
