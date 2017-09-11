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

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

/**
 * Configuration factory that populates an {@link Configuration} with static and runtime
 * configuration settings.
 */
public class TestPropertyPolicy implements PropertyPolicy {
  final static Set<String> propertiesKeyDisAllowed =
      Collections.unmodifiableSet(new HashSet<>(asList("DisallowedProperty")));

  @Override public void evaluate(Properties properties) throws PropertyNotAllowedException {
    final Properties offendingProperties = new Properties();

    for (Object k : properties.keySet()) {
      if (propertiesKeyDisAllowed.contains(k))
        offendingProperties.put(k, properties.getProperty((String) k));
    }

    if (offendingProperties.size() > 0) throw new PropertyNotAllowedException(offendingProperties);
  }
}