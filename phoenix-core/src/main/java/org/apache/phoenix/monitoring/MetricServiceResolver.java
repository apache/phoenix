/**
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

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.InstanceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class helps resolve the metricpublisher supplier class at the run time.
 * Based on the classString name passed, it will return the appropriate class Instance.
 */
public class MetricServiceResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricServiceResolver.class);

    private MetricPublisherSupplierFactory metricSupplier = null;

    public MetricPublisherSupplierFactory instantiate(String classString) {
        Preconditions.checkNotNull(classString);
        if (metricSupplier == null) {
            try {
                Class clazz = Class.forName(classString);
                List<MetricPublisherSupplierFactory>
                        factoryList =
                        InstanceResolver.get(MetricPublisherSupplierFactory.class, null);
                for (MetricPublisherSupplierFactory factory : factoryList) {
                    if (clazz.isInstance(factory)) {
                        metricSupplier = factory;
                        LOGGER.info(String.format(
                                "Sucessfully loaded class for MetricPublishFactory of type: %s",
                                classString));
                        break;
                    }
                }
                if (metricSupplier == null) {
                    String msg = String.format("Could not load/instantiate class %s", classString);
                    LOGGER.error(msg);
                }
            } catch (ClassNotFoundException e) {
                LOGGER.error(String.format("Could not load/instantiate class %s", classString), e);
            }
        }
        return metricSupplier;
    }
}
