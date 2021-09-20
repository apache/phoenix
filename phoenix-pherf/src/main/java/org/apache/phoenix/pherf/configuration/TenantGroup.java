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

package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlAttribute;

public class TenantGroup {
    public static final String DEFAULT_GLOBAL_ID = "GLOBAL";
    private String id;
    private int weight;
    private int numTenants;
    private boolean useGlobalConnection;

    @XmlAttribute
    public String getId() {
        return useGlobalConnection ? DEFAULT_GLOBAL_ID: id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @XmlAttribute
    public int getWeight() {
        return useGlobalConnection ? 100 : weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @XmlAttribute
    public int getNumTenants() { return useGlobalConnection ? 1 : numTenants; }

    public void setNumTenants(int numTenants) { this.numTenants = numTenants; }

    @XmlAttribute
    public boolean isUseGlobalConnection() {
        return useGlobalConnection;
    }

    public void setUseGlobalConnection(boolean useGlobalConnection) {
        this.useGlobalConnection = useGlobalConnection;
    }

}
