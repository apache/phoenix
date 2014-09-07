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

import org.apache.hadoop.hbase.KeyValue;

/**
 * Track a statistic for the column on a given region
 */
public interface StatisticsTracker {

    /**
     * Reset the statistic after the completion of the compaction
     */
    public void clear();

    /**
     * Update the current statistics with the next {@link KeyValue} to be written
     * 
     * @param kv
     *            next {@link KeyValue} to be written.
     */
    public void updateStatistic(KeyValue kv);

    /**
     * Return the max key of the family
     * @param fam
     * @return
     */
    public byte[] getMaxKey(String fam);

    /**
     * Return the min key of the family
     * 
     * @param fam
     * @return
     */
    public byte[] getMinKey(String fam);

    /**
     * Return the guide posts of the family
     * 
     * @param fam
     * @return
     */
    public byte[] getGuidePosts(String fam);
}