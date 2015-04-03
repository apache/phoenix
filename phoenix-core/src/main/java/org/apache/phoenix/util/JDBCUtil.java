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

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static org.apache.phoenix.util.PhoenixRuntime.ANNOTATION_ATTRIB_PREFIX;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;

import com.google.common.base.Preconditions;
import com.sun.istack.NotNull;



/**
 * Utilities for JDBC
 *
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
        String urlPropName = PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + propName.toUpperCase() + "=";
        String upperCaseURL = url.toUpperCase();
        String propValue = info.getProperty(propName);
        if (propValue == null) {
            int begIndex = upperCaseURL.indexOf(urlPropName);
            if (begIndex >= 0) {
                int endIndex =
                        upperCaseURL.indexOf(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR, begIndex
                                + urlPropName.length());
                if (endIndex < 0) {
                    endIndex = url.length();
                }
                propValue = url.substring(begIndex + urlPropName.length(), endIndex);
            }
        }
        return propValue;
    }

    public static String removeProperty(String url, String propName) {
        String urlPropName = PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + propName.toUpperCase() + "=";
        String upperCaseURL = url.toUpperCase();
        int begIndex = upperCaseURL.indexOf(urlPropName);
        if (begIndex >= 0) {
            int endIndex =
                    upperCaseURL.indexOf(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR, begIndex
                            + urlPropName.length());
            if (endIndex < 0) {
                endIndex = url.length();
            }
            String prefix = url.substring(0, begIndex);
            String suffix = url.substring(endIndex, url.length());
            return prefix + suffix;
        } else {
            return url;
        }
    }

    /**
     * Returns a map that contains connection properties from both <code>info</code> and <code>url</code>.
     */
    private static Map<String, String> getCombinedConnectionProperties(String url, Properties info) {
		Map<String, String> result = newHashMapWithExpectedSize(info.size());
		for (String propName : info.stringPropertyNames()) {
			result.put(propName, info.getProperty(propName));
		}
		String[] urlPropNameValues = url.split(Character.toString(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR));
		if (urlPropNameValues.length > 1) {
			for (int i = 1; i < urlPropNameValues.length; i++) {
				String[] urlPropNameValue = urlPropNameValues[i].split("=");
				if (urlPropNameValue.length == 2) {
					result.put(urlPropNameValue[0], urlPropNameValue[1]);
				}
			}
		}
		
		return result;
    }
    
    public static Map<String, String> getAnnotations(@NotNull String url, @NotNull Properties info) {
        Preconditions.checkNotNull(url);
        Preconditions.checkNotNull(info);
        
    	Map<String, String> combinedProperties = getCombinedConnectionProperties(url, info);
    	Map<String, String> result = newHashMapWithExpectedSize(combinedProperties.size());
    	for (Map.Entry<String, String> prop : combinedProperties.entrySet()) {
    		if (prop.getKey().startsWith(ANNOTATION_ATTRIB_PREFIX) &&
    				prop.getKey().length() > ANNOTATION_ATTRIB_PREFIX.length()) {
    			result.put(prop.getKey().substring(ANNOTATION_ATTRIB_PREFIX.length()), prop.getValue());
    		}
    	}
    	return result;
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

    /**
     * Retrieve the value of the optional auto-commit setting from JDBC url or connection
     * properties.
     *
     * @param url JDBC url used for connecting to Phoenix
     * @param info connection properties
     * @param defaultValue default to return if the auto-commit property is not set in the url
     *                     or connection properties
     * @return the boolean value supplied for the AutoCommit in the connection URL or properties,
     * or the supplied default value if no AutoCommit attribute was provided
     */
    public static boolean getAutoCommit(String url, Properties info, boolean defaultValue) {
        String autoCommit = findProperty(url, info, PhoenixRuntime.AUTO_COMMIT_ATTRIB);
        if (autoCommit == null) {
            return defaultValue;
        }
        return Boolean.valueOf(autoCommit);
    }

    /**
     * Retrieve the value of the optional consistency read setting from JDBC url or connection
     * properties.
     *
     * @param url JDBC url used for connecting to Phoenix
     * @param info connection properties
     * @param defaultValue default to return if ReadConsistency property is not set in the url
     *                     or connection properties
     * @return the boolean value supplied for the AutoCommit in the connection URL or properties,
     * or the supplied default value if no AutoCommit attribute was provided
     */
    public static Consistency getConsistencyLevel(String url, Properties info, String defaultValue) {
        String consistency = findProperty(url, info, PhoenixRuntime.CONSISTENCY_ATTRIB);

        if(consistency != null && consistency.equalsIgnoreCase(Consistency.TIMELINE.toString())){
            return Consistency.TIMELINE;
        }

        return Consistency.STRONG;
    }
}
