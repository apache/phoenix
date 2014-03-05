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

import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;



/**
 * Utilities for JDBC
 *
 * 
 * @since 178
 */
public class JDBCUtil {
    
    private JDBCUtil() {
    }

    /**
     * Find the propName by first looking in the url string and if not found,
     * next in the info properties. If not found, null is returned.
     * @param url JDBC connection URL
     * @param info JDBC connection properties
     * @param propName the name of the property to find
     * @return the property value or null if not found
     */
    public static String findProperty(String url, Properties info, String propName) {
        String urlPropName = ";" + propName + "=";
        String propValue = info.getProperty(propName);
        if (propValue == null) {
            int begIndex = url.indexOf(urlPropName);
            if (begIndex >= 0) {
                int endIndex = url.indexOf(';',begIndex + urlPropName.length());
                if (endIndex < 0) {
                    endIndex = url.length();
                }
                propValue = url.substring(begIndex + urlPropName.length(), endIndex);
            }
        }
        return propValue;
    }

    public static Long getCurrentSCN(String url, Properties info) throws SQLException {
        String scnStr = findProperty(url, info, PhoenixRuntime.CURRENT_SCN_ATTRIB);
        return (scnStr == null ? null : Long.parseLong(scnStr));
    }

    public static int getMutateBatchSize(String url, Properties info, ReadOnlyProps props) throws SQLException {
        String batchSizeStr = findProperty(url, info, PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB);
        return (batchSizeStr == null ? props.getInt(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE) : Integer.parseInt(batchSizeStr));
    }

    public static @Nullable PName getTenantId(String url, Properties info) throws SQLException {
        String tenantId = findProperty(url, info, PhoenixRuntime.TENANT_ID_ATTRIB);
        return (tenantId == null ? null : PNameFactory.newName(tenantId));
    }
}
