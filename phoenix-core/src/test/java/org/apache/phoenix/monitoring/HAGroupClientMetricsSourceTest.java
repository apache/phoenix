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
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.metrics.MetricConstants.HA_GROUP_TAG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import javax.management.ObjectName;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for {@link HAGroupClientMetricsSource}: the per-HA-group metrics2 source stamps a
 * {@code ha_group} tag carrying the group name, keeps per-group counters, and detaches cleanly on
 * {@link HAGroupClientMetricsSource#unregister()}.
 * <p>
 * Constructing a source registers it with the process-global {@link DefaultMetricsSystem}, so each
 * test uses a unique group name and {@link #tearDown()} unregisters every source it created.
 */
public class HAGroupClientMetricsSourceTest {

  private final List<HAGroupClientMetricsSource> created = new ArrayList<>();

  private HAGroupClientMetricsSource newSource(String group) {
    HAGroupClientMetricsSource source = new HAGroupClientMetricsSource(group);
    created.add(source);
    return source;
  }

  @After
  public void tearDown() {
    for (HAGroupClientMetricsSource source : created) {
      source.unregister();
    }
    created.clear();
  }

  @Test
  public void testTagCarriesUnquotedGroupName() {
    String group = "srcTag";
    HAGroupClientMetricsSource source = newSource(group);
    MetricsTag tag = source.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME);
    assertNotNull("the ha_group tag must be present on the source", tag);
    assertEquals("ha_group tag must carry the unquoted group name", group, tag.value());
  }

  @Test
  public void testJmxContextIsQuotedPerGroup() {
    String group = "group,one=weird";
    HAGroupClientMetricsSource source = newSource(group);
    assertEquals(group, source.getMetricsRegistry().getTag(HA_GROUP_TAG_NAME).value());
    assertTrue(source.getMetricsJmxContext().endsWith(",haGroup=" + ObjectName.quote(group)));
  }

  @Test
  public void testIncrementAndUpdateArePerCounter() {
    HAGroupClientMetricsSource source = newSource("srcCounters");
    source.increment(MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER);
    source.increment(MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER);
    // The duration metric accumulates via update(); other counters are untouched by it.
    source.update(MetricType.HA_FAILOVER_DURATION_MS, 40L);
    source.update(MetricType.HA_FAILOVER_DURATION_MS, 60L);

    assertEquals(2L, source.getCounterValue(MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER));
    assertEquals(100L, source.getCounterValue(MetricType.HA_FAILOVER_DURATION_MS));
    assertEquals("an untouched counter stays at zero", 0L,
      source.getCounterValue(MetricType.HA_STALE_CRR_DETECTED_COUNT));
  }

  @Test
  public void testUnknownMetricTypeIsIgnored() {
    HAGroupClientMetricsSource source = newSource("srcUnknown");
    // A type that is not in METRIC_TYPES has no backing counter: increment/update are no-ops and
    // getCounterValue reports -1 to distinguish "absent" from a present-but-zero counter.
    source.increment(MetricType.MUTATION_BATCH_SIZE);
    source.update(MetricType.MUTATION_BATCH_SIZE, 5L);
    assertEquals(-1L, source.getCounterValue(MetricType.MUTATION_BATCH_SIZE));
  }

  @Test
  public void testEachGroupIsADistinctRegisteredSource() {
    HAGroupClientMetricsSource a = newSource("srcDistinctA");
    HAGroupClientMetricsSource b = newSource("srcDistinctB");
    assertNotEquals("distinct groups must get distinct JMX contexts", a.getMetricsJmxContext(),
      b.getMetricsJmxContext());
    assertNotNull(DefaultMetricsSystem.instance().getSource(a.getMetricsJmxContext()));
    assertNotNull(DefaultMetricsSystem.instance().getSource(b.getMetricsJmxContext()));
  }

  @Test
  public void testUnregisterFreesTheSourceNameForReuse() {
    String group = "srcReuse";
    HAGroupClientMetricsSource first = new HAGroupClientMetricsSource(group);
    String jmxContext = first.getMetricsJmxContext();
    assertNotNull(DefaultMetricsSystem.instance().getSource(jmxContext));

    first.unregister();
    assertNull("unregister must free the source name in DefaultMetricsSystem",
      DefaultMetricsSystem.instance().getSource(jmxContext));

    // The same group name must be registrable again (would throw "source already exists" if the
    // prior source were still attached).
    HAGroupClientMetricsSource second = newSource(group);
    assertNotNull(DefaultMetricsSystem.instance().getSource(second.getMetricsJmxContext()));
    assertEquals(jmxContext, second.getMetricsJmxContext());
  }
}
