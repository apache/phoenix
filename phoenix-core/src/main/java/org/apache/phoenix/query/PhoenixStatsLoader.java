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
package org.apache.phoenix.query;

import org.apache.phoenix.schema.stats.GuidePostsInfo;
import org.apache.phoenix.schema.stats.GuidePostsKey;

/**
 * The interface for talking to underneath layers to load stats from stats table for a given key
 */
public interface PhoenixStatsLoader {
    /**
     * Use to check whether this is the time to load stats from stats table.
     * There are two cases:
     * a. After a specified duration has passed
     * b. The stats on server side (e.g. in stats table) has been updated
     *
     * @return boolean indicates whether we need to load stats or not
     */
    boolean needsLoad();

    /**
     * Called by client stats cache to load stats from underneath layers
     *
     * @param statsKey the stats key used to search the stats on server side (in stats table)
     * @throws Exception
     *
     * @return GuidePostsInfo retrieved from sever side
     */
    GuidePostsInfo loadStats(GuidePostsKey statsKey) throws Exception;

    /**
     * Called by client stats cache to load stats from underneath layers
     *
     * @param statsKey the stats key used to search the stats on server side (in stats table)
     * @param prevGuidepostInfo the existing stats cached on the client side or GuidePostsInfo.NO_GUIDEPOST
     * @throws Exception
     *
     * @return GuidePostsInfo retrieved from sever side
     */
    GuidePostsInfo loadStats(GuidePostsKey statsKey, GuidePostsInfo prevGuidepostInfo) throws Exception;
}
