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

package org.apache.phoenix.queryserver.metrics;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.monitoring.PhoenixQueryServerSink;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class PhoenixQueryServerGlobalMetrics {

    @Inject
    Configuration configuration;

    @Inject
    private PhoenixQueryServerSink phoenixQueryServerSink;

    public void scheduleGlobalMetricsCollection() {
        int delay = configuration.getInt(QueryServices.PHOENIX_PQS_GLOBAL_INITIAL_DELAY,
                QueryServicesOptions.DEFAULT_PHOENIX_PQS_GLOBAL_INITIAL_DELAY);
        int interval = configuration.getInt(QueryServices.PHOENIX_PQS_GLOBAL_PERIOD,
                QueryServicesOptions.DEFAULT_PHOENIX_PQS_GLOBAL_PERIOD);

        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Collection<GlobalMetric> metrics = PhoenixRuntime.getGlobalPhoenixClientMetrics();
                phoenixQueryServerSink.putGlobalMetrics(metrics);

            }
        }, delay, interval, TimeUnit.SECONDS);
    }


}
