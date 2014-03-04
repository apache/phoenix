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

import java.sql.SQLException;

import org.apache.phoenix.schema.TableRef;


/**
 * 
 * Interface for managing and caching table statistics.
 * The frequency of updating the table statistics are controlled
 * by {@link org.apache.phoenix.query.QueryServices#STATS_UPDATE_FREQ_MS_ATTRIB}.
 * Table stats may also be manually updated through {@link #updateStats(TableRef)}.
 * 
 *
 * 
 * @since 0.1
 */
public interface StatsManager {
    /**
     * Get the minimum key for the given table
     * @param table the table
     * @return the minimum key or null if unknown
     */
    byte[] getMinKey(TableRef table);
    
    /**
     * Get the maximum key for the given table
     * @param table the table
     * @return the maximum key or null if unknown
     */
    byte[] getMaxKey(TableRef table);
    
    /**
     * Manually update the cached table statistics
     * @param table the table
     * @throws SQLException
     */
    void updateStats(TableRef table) throws SQLException;
    
    void clearStats() throws SQLException;
}
