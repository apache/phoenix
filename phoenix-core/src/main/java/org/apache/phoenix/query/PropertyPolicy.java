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
package org.apache.phoenix.query;
import java.util.Properties;

/**
 * Filters {@link Properties} instances based on property policy.
 *
 * Provided properties, check if each property inside is whitelisted,
 * if not, throws IllegalArgumentException.
 * For best code practice, throws the offending properties list along
 * with exception
 *
 * An example will be:
 *<pre>
 *{@code 
 *public class Customized PropertyPolicy implements PropertyPolicy {
 *  final static Set<String> propertiesKeyAllowed = Collections.unmodifiableSet(
 *      new HashSet<>(asList("DriverMajorVersion","DriverMinorVersion","DriverName","CurrentSCN")));
 *
 *      public void evaluate(Properties properties) {
 *      final Set<String> offendingProperties = new HashSet<>();
 *
 *      for(Object k:properties.keySet()){
 *          if (propertiesKeyDisAllowed.contains(k)) offendingProperties
 *          .put((String)k,properties.getProperty((String)k));
 *      }
 *
 *      if (offendingProperties.size()>0) throw new IllegalArgumentException(
 *      "properties not allowed. offending properties" + offendingProperties);
 *  }
 *}
 *}
 *</pre>
 */
public interface PropertyPolicy {
    /**
     * @param properties
     * @throws IllegalArgumentException
     */
    void evaluate(Properties properties) throws PropertyNotAllowedException;

    /**
     * Default implementation allows all properties.
     */
    static class PropertyPolicyImpl implements PropertyPolicy {
        @Override
        public void evaluate(Properties properties) throws PropertyNotAllowedException{}
    }
}
