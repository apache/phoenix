/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.iterate;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

/**
 * Scan list to be retrieved for the BaseResultIterator with the list of region locations the scans
 * would be served from.
 */
public class ScansWithRegionLocations {

    private final List<List<Scan>> scans;
    private final List<HRegionLocation> regionLocations;

    public ScansWithRegionLocations(List<List<Scan>> scans,
                                    List<HRegionLocation> regionLocations) {
        this.scans = scans;
        this.regionLocations = regionLocations;
    }

    public List<List<Scan>> getScans() {
        return scans;
    }

    public List<HRegionLocation> getRegionLocations() {
        return regionLocations;
    }
}
