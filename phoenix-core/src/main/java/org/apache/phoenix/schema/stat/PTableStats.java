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
package org.apache.phoenix.schema.stat;

import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;


/**
 * Interface for Phoenix table statistics. Statistics is collected on the server
 * side and can be used for various purpose like splitting region for scanning, etc.
 * 
 * The table is defined on the client side, but it is populated on the server side. The client
 * should not populate any data to the statistics object.
 */
public interface PTableStats {

    /**
     * Given the region info, returns an array of bytes that is the current estimate of key
     * distribution inside that region. The keys should split that region into equal chunks.
     * 
     * @param region
     * @return array of keys
     */
    byte[][] getRegionGuidePosts(HRegionInfo region);

    Map<String, byte[][]> getGuidePosts();
}
