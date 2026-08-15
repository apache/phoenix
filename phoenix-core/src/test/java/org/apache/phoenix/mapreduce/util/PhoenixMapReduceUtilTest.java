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
package org.apache.phoenix.mapreduce.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class PhoenixMapReduceUtilTest {

  @Test
  public void testAddPhoenixDependencyJars() throws Exception {
    Configuration conf = new Configuration();
    PhoenixMapReduceUtil.addPhoenixDependencyJars(conf);
    String tmpjars = conf.get("tmpjars");
    assertNotNull("tmpjars should not be null", tmpjars);
    // Classes loaded from target/classes (same build) won't resolve to a jar,
    // so we only assert on external dependencies that are actual jars on the classpath.
    assertTrue("Should contain phoenix-shaded-guava", tmpjars.contains("phoenix-shaded-guava"));
    assertTrue("Should contain joda-time", tmpjars.contains("joda-time"));
    assertTrue("Should contain antlr-runtime", tmpjars.contains("antlr-runtime"));
    assertTrue("Should contain htrace-core", tmpjars.contains("htrace-core"));
    assertTrue("Should contain hbase-metrics-api", tmpjars.contains("hbase-metrics-api"));
    assertTrue("Should contain hbase-metrics", tmpjars.contains("hbase-metrics"));
    assertTrue("Should contain disruptor", tmpjars.contains("disruptor"));
    assertTrue("Should contain jackson-core", tmpjars.contains("jackson-core"));
    assertTrue("Should contain jackson-databind", tmpjars.contains("jackson-databind"));
    assertTrue("Should contain jackson-annotations", tmpjars.contains("jackson-annotations"));
    assertTrue("Should contain commons-csv", tmpjars.contains("commons-csv"));
    assertTrue("Should contain json-path", tmpjars.contains("json-path"));
    assertTrue("Should contain bson", tmpjars.contains("bson"));
  }
}
