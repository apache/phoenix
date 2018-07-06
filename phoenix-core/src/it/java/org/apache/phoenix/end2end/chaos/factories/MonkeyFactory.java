/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.end2end.chaos.factories;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import com.google.common.collect.ImmutableMap;

/**
 * Phoenix specific monkey factory. Supports a set of chaos monkeys that use
 * hbase-it chaos actions but in combinations that make sense for Phoenix tests.
 * <p>For example, Phoenix utilizes multiple tables for global secondary indexes
 * so we cannot not use monkeys that can protect only a single table while
 * deleting others randomly. Nor can we use monkeys that mutate schema randomly.
 * When building our monkeys we avoid such actions.
 */
public abstract class MonkeyFactory {

    private static final Log LOG = LogFactory.getLog(MonkeyFactory.class);

    public static final String CALM = "calm";
    public static final String SLOW_DETERMINISTIC = "slowDeterministic";
    public static final String SERVER_KILLING = "serverKilling";
    public static final String NO_KILL = "noKill";

    protected Properties properties;
    protected IntegrationTestingUtility util;

    private static Map<String, MonkeyFactory> FACTORIES = ImmutableMap.<String,MonkeyFactory>builder()
        .put(CALM, new CalmMonkeyFactory())
        .put(SLOW_DETERMINISTIC, new SlowDeterministicMonkeyFactory())
        .put(SERVER_KILLING, new ServerKillingMonkeyFactory())
        .put(NO_KILL, new NoKillMonkeyFactory())
        .build();

    public abstract ChaosMonkey build();

    public MonkeyFactory setProperties(Properties props) {
        this.properties = props;
        return this;
    }

    public MonkeyFactory setUtil(IntegrationTestingUtility util) {
        this.util = util;
        return this;
    }

    public static MonkeyFactory getFactory(String factoryName) {
        MonkeyFactory fact = FACTORIES.get(factoryName);
        if (fact == null && factoryName != null && !factoryName.isEmpty()) {
          Class<?> klass = null;
          try {
              klass = Class.forName(factoryName);
              if (klass != null) {
                  fact = (MonkeyFactory) ReflectionUtils.newInstance(klass);
              }
          } catch (Exception e) {
              LOG.error("Error trying to create " + factoryName + " could not load it by class name");
              return null;
          }
        }
        return fact;
    }

}
