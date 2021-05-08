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

package org.apache.phoenix.pherf.workload.mt.generators;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;

import java.util.List;
import java.util.Properties;

/**
 * An interface that factory implementers need to implement
 * for creating various supported load generators {@link LoadEventGenerator}
 * @param <T> load event object
 */
public interface LoadEventGeneratorFactory<T> {
    LoadEventGenerator<T> newLoadEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            Properties properties) ;

    LoadEventGenerator<T> newLoadEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) ;

}
