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

import java.util.Properties;

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
}
