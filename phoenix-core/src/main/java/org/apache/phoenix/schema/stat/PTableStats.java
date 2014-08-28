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


/*
 * The table is defined on the client side, but it is populated on the server side. The client should not populate any data to the
 * statistics object.
 */
public interface PTableStats {

    /**
     * Given the region info, returns an array of bytes that is the current estimate of key distribution inside that region. The keys should
     * split that region into equal chunks.
     * 
     * @param region
     * @return array of keys
     */
    byte[] getRegionGuidePostsOfARegion(HRegionInfo region);

    /**
     * Returns a map of with key as the regions of the table and the guide posts collected as byte[] created from VARBINARY ARRAY
     * 
     * @return map of region and the guide posts as VARBINARY array
     */
    Map<String, byte[]> getGuidePosts();

    /**
     * Returns the max key of a region
     * 
     * @param region
     * @return maxkey of a region
     */
    byte[] getMaxKeyOfARegion(HRegionInfo region);

    /**
     * Returns the min key of a region
     * 
     * @param region
     * @return min key of a region
     */
    byte[] getMinKeyOfARegion(HRegionInfo region);

    /**
     * Returns a map with key as the regions of the table and the max key in that region
     * 
     * @return
     */
    Map<String, byte[]> getMaxKey();

    /**
     * Returns a map with key as the regions of the table and the min key in that region
     * 
     * @return
     */
    Map<String, byte[]> getMinKey();

    /**
     * Set the min key of a region
     * 
     * @param region
     * @param minKey
     * @param offset
     * @param length
     */
    void setMinKey(HRegionInfo region, byte[] minKey, int offset, int length);

    /**
     * Set the min key of the region. Here the region name is represented as the Bytes.toBytes(region.getRegionNameAsString).
     * 
     * @param region
     * @param minKey
     * @param offset
     * @param length
     */
    void setMinKey(byte[] region, byte[] minKey, int offset, int length);

    /**
     * Set the max key of a region
     * 
     * @param region
     * @param maxKey
     * @param offset
     * @param length
     */
    void setMaxKey(HRegionInfo region, byte[] maxKey, int offset, int length);

    /**
     * Set the max key of a region. Here the region name is represented as the Bytes.toBytes(region.getRegionNameAsString).
     * 
     * @param region
     * @param maxKey
     * @param offset
     * @param length
     */
    void setMaxKey(byte[] region, byte[] maxKey, int offset, int length);

    /**
     * Set the guide posts for a region
     * 
     * @param region
     * @param guidePosts
     * @param offset
     * @param length
     */
    void setGuidePosts(HRegionInfo region, byte[] guidePosts, int offset, int length);

    /**
     * Set the guide posts of a region. Here the region name is represented as the Bytes.toBytes(region.getRegionNameAsString).
     * 
     * @param region
     * @param guidePosts
     * @param offset
     * @param length
     */
    void setGuidePosts(byte[] region, byte[] guidePosts, int offset, int length);

}