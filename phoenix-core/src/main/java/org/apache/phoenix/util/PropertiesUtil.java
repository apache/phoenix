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

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;

public class PropertiesUtil {

    private PropertiesUtil() {
    }
    
    /**
     * Use this to deep copy properties. The copy constructor in {@link java.util.Properties} does not do a deep copy.
     * @param properties
     * @return new mutable instance of Properties populated with values from the passed in Properties.
     */
    public static Properties deepCopy(Properties properties) {
        Properties newProperties = new Properties();
        for (String pName : properties.stringPropertyNames()) {
            newProperties.setProperty(pName, properties.getProperty(pName));
        }
        return newProperties;
    }
    
    /**
     * Add properties from the given Configuration to the provided Properties. Note that only those
     * configuration properties will be added to the provided properties whose values are already
     * not set. The method doesn't modify the passed in properties instead makes a clone of them
     * before combining.
     * @return properties object that is a combination of properties contained in props and
     *         properties contained in conf
     */
    public static Properties combineProperties(Properties props, final Configuration conf) {
        return combineProperties(props, conf, Collections.<String>emptySet());
    }

    public static Properties combineProperties(Properties props, final Configuration conf, Set<String> withoutTheseProps) {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        Properties copy = deepCopy(props);
        if (iterator != null) {
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                // set the property from config only if props doesn't have it already
                if (copy.getProperty(entry.getKey()) == null && !withoutTheseProps.contains(entry.getKey())) {
                    copy.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
        return copy;
    }

   /**
     * Utility to work around the limitation of the copy constructor
     * {@link Configuration#Configuration(Configuration)} provided by the {@link Configuration}
     * class. See https://issues.apache.org/jira/browse/HBASE-18378.
     * The copy constructor doesn't copy all the config settings, so we need to resort to
     * iterating through all the settings and setting it on the cloned config.
     * @param toCopy  configuration to copy
     * @return
     */
    public static Configuration cloneConfig(Configuration toCopy) {
        Configuration clone = new Configuration();
        Iterator<Entry<String, String>> iterator = toCopy.iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            clone.set(entry.getKey(), entry.getValue());
        }
        return clone;
    }
}
