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


import org.apache.phoenix.queryserver.metrics.sink.PqsFilePqsSink;
import org.apache.phoenix.queryserver.metrics.sink.PqsSink;
import org.apache.phoenix.queryserver.metrics.sink.PqsSlf4jSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PqsMetricsSystem {

    public final static String statementLevelMetrics = "statement";
    public final static String connectionMetrics = "connection";
    protected static final Logger LOG = LoggerFactory.getLogger(PqsMetricsSystem.class);

    public PqsSink pqsSink;


    public PqsMetricsSystem(){
        Thread globalMetricThread = null;
        PqsGlobalMetrics pqsGlobalMetricsToJMX = null;
        try {
            pqsSink = getSinkObject();
            pqsGlobalMetricsToJMX = new PqsGlobalMetrics(pqsSink);
            globalMetricThread = new Thread(pqsGlobalMetricsToJMX);
            globalMetricThread.start();
        }catch (Exception ex){
            LOG.error(" could not instantiate the PQS Metrics System");
            if (globalMetricThread!=null) {
                try {
                    globalMetricThread.join();
                } catch (Exception ine) {
                    LOG.error(" unable to stop the global metrics thread",ine);
                }
            }

        }
     }

      private PqsSink getSinkObject(){
        PqsSink pqsSink;
        String typeOfSink = PqsConfiguration.getTypeOfSink();
        switch(typeOfSink) {
            case "file":
                pqsSink = new PqsFilePqsSink();
                break;
            case "slf4j": pqsSink = new PqsSlf4jSink() ; break;
            //default is also LOG file.
            default: pqsSink = new PqsSlf4jSink() ; break;
        }
        return pqsSink;
    }




    }

