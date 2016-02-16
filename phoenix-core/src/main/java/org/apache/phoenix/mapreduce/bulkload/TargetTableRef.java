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
package org.apache.phoenix.mapreduce.bulkload;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.google.common.collect.Maps;

/**
 * Represents the logical and physical name of a single table to which data is to be loaded.
 *
 * This class exists to allow for the difference between HBase physical table names and
 * Phoenix logical table names.
 */
public class TargetTableRef {

    @JsonProperty
    private final String logicalName;

    @JsonProperty
    private final String physicalName;

    @JsonProperty
    private Map<String,String> configuration = Maps.newHashMap();

    public TargetTableRef(String name) {
        this(name, name);
    }

    @JsonCreator
    public TargetTableRef(@JsonProperty("logicalName") String logicalName,
        @JsonProperty("physicalName") String physicalName) {
        this.logicalName = logicalName;
        this.physicalName = physicalName;
    }

    public String getLogicalName() {
        return logicalName;
    }

    public String getPhysicalName() {
        return physicalName;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }
}
