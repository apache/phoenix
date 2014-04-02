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
package org.apache.phoenix.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.cache.GlobalCache;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;


/**
 * Coprocessor for metadata related operations. This coprocessor would only be registered
 * to SYSTEM.TABLE.
 */
public class MetaDataRegionObserver extends BaseRegionObserver {

    @Override
    public void preClose(final ObserverContext<RegionCoprocessorEnvironment> c,
            boolean abortRequested) {
        GlobalCache.getInstance(c.getEnvironment()).getMetaDataCache().invalidateAll();
    }
    
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      // sleep a little bit to compensate time clock skew when SYSTEM.CATALOG moves 
      // among region servers because we relies on server time of RS which is hosting
      // SYSTEM.CATALOG
      long sleepTime = env.getConfiguration().getLong(QueryServices.CLOCK_SKEW_INTERVAL_ATTRIB, 
          QueryServicesOptions.DEFAULT_CLOCK_SKEW_INTERVAL);
      try {
          if(sleepTime > 0) {
              Thread.sleep(sleepTime);
          }
      } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
      }
    }
}
