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

package org.apache.phoenix.queryserver.metrics.sink;


import static org.apache.phoenix.queryserver.metrics.PqsMetricsSystem.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PqsSlf4jSink extends PqsSink {

    private Logger logger = LoggerFactory.getLogger(PqsSlf4jSink.class);;

    public PqsSlf4jSink(){
    }

    @Override
    public void close()  {}

    @Override
    public void writeJson(String json, MetricType type){
        // if debug is enabled log global and request level metrics
        // if info is enabled log only request level metrics
        if (logger.isDebugEnabled()) {
            logger.debug(json);
        } else if (logger.isInfoEnabled()){
            if (type == MetricType.request) {
                logger.info(json);
            }
        }
    }

}
