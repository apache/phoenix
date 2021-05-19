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
package org.apache.phoenix.monitoring;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PhoenixTableMetricImplTest {

    /*
    Tests the functionality of the TableMetricImpl methods which are exposed
    1. change()
    2. increment()
    3. decrement()
    4. reset()
    5. getValue()
     */
    @Test public void testTableMetricImplAvailableMethods() {

        PhoenixTableMetric metric = new PhoenixTableMetricImpl(MetricType.SELECT_SQL_COUNTER);

        for (int i = 0; i < 10; i++) {
            metric.increment();
        }
        assertEquals(10, metric.getValue());
        metric.reset();
        assertEquals(0, metric.getValue());

        for (int i = 0; i < 5; i++) {
            metric.change(i);
        }
        assertEquals(10, metric.getValue());
        metric.reset();
        assertEquals(0, metric.getValue());

        metric.change(10);
        assertEquals(10, metric.getValue());
        for (int i = 0; i < 5; i++) {
            metric.decrement();
        }
        assertEquals(5, metric.getValue());
    }

    @Test public void testPhoenixImplchange() {

        PhoenixTableMetric metric = new PhoenixTableMetricImpl(MetricType.SELECT_SQL_COUNTER);
        for (int i = 0; i < 5; i++) {
            metric.change(i);
        }
        assertEquals(10, metric.getValue());
    }

    @Test public void testPhoenixImplIncrement() {
        PhoenixTableMetric metric = new PhoenixTableMetricImpl(MetricType.SELECT_SQL_COUNTER);
        for (int i = 0; i < 10; i++) {
            metric.increment();
        }
        assertEquals(10, metric.getValue());
    }

    @Test public void testPhoenixImplDecrement() {
        PhoenixTableMetric metric = new PhoenixTableMetricImpl(MetricType.SELECT_SQL_COUNTER);
        metric.change(10);
        for (int i = 0; i < 5; i++) {
            metric.decrement();
        }
        assertEquals(5, metric.getValue());
    }

    @Test public void testPhoenixImplReset() {
        PhoenixTableMetric metric = new PhoenixTableMetricImpl(MetricType.SELECT_SQL_COUNTER);
        metric.change(10);
        metric.reset();
        assertEquals(0, metric.getValue());
    }

    @Test public void testPhoenixImplGetValue() {
        PhoenixTableMetric metric = new PhoenixTableMetricImpl(MetricType.SELECT_SQL_COUNTER);
        metric.change(10);
        assertEquals(10, metric.getValue());
    }

}
