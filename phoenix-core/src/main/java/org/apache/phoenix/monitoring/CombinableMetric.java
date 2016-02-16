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



/**
 * Interface for representing a metric that could be published and possibly combined with a metric of the same
 * type.
 */
public interface CombinableMetric extends Metric {

    String getPublishString();

    CombinableMetric combine(CombinableMetric metric);

    public class NoOpRequestMetric implements CombinableMetric {

        public static NoOpRequestMetric INSTANCE = new NoOpRequestMetric();
        private static final String EMPTY_STRING = "";

        @Override
        public String getName() {
            return EMPTY_STRING;
        }

        @Override
        public String getDescription() {
            return EMPTY_STRING;
        }

        @Override
        public long getValue() {
            return 0;
        }

        @Override
        public void change(long delta) {}

        @Override
        public void increment() {}

        @Override
        public String getCurrentMetricState() {
            return EMPTY_STRING;
        }

        @Override
        public void reset() {}

        @Override
        public String getPublishString() {
            return EMPTY_STRING;
        }

        @Override
        public CombinableMetric combine(CombinableMetric metric) {
            return INSTANCE;
        }

        @Override
        public void decrement() {}
    }

}
