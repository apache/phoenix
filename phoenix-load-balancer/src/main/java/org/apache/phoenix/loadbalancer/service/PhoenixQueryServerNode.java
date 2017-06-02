/**
 *
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
package org.apache.phoenix.loadbalancer.service;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonRootName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Immutable class for defining the server location for
 * Phoenix query instance. This data is stored as Node data
 * in zookeeper
 */
public class PhoenixQueryServerNode {

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = port;
    }

    private String host;
    private String port;

    public PhoenixQueryServerNode(String host, String port){
        this.host = host;
        this.port = port;
    }

    public PhoenixQueryServerNode(){
        super();
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("%s:%s",host,port);
    }

    public String toJsonString() throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(this);
    }

    public static PhoenixQueryServerNode fromJsonString(String jsonString) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString,PhoenixQueryServerNode.class);
    }


}
