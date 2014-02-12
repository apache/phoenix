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
package org.apache.phoenix.join;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;


/**
 * 
 * Prototype for region observer that performs a hash join between two tables.
 * The client sends over the rows in a serialized format and the coprocessor
 * deserialized into a Map and caches it on the region server.  The map is then
 * used to resolve the foreign key reference and the rows are then joined together.
 *
 * TODO: Scan rows locally on region server instead of returning to client
 * if we can know that all both tables rows are on the same region server.
 * 
 * FIXME: For now this does nothing. In it's previous state for 0.94.4 it was not
 * able to instantiate it (it was fine in 0.94.2). Since we're not yet using it,
 * changing it to do nothing. Once we add joins, we'll need to figure this out.
 * I suspect it is not being able to be instantiated because of Snappy.
 * 
 * 
 * @since 0.1
 */
public class HashJoiningRegionObserver extends BaseScannerRegionObserver  {

    @Override
    protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
            RegionScanner s) throws IOException {
        return s;
    }
}
