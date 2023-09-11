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
package org.apache.phoenix.hbase.index.covered;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.ReplayWrite;
import org.apache.phoenix.util.ScanUtil;

public interface IndexMetaData {

    public static final IndexMetaData NULL_INDEX_META_DATA = new IndexMetaData() {

        @Override
        public boolean requiresPriorRowState(Mutation m) {
            return true;
        }

        @Override
        public ReplayWrite getReplayWrite() {
          return null;
        }

        @Override
        public int getClientVersion() {
            return ScanUtil.UNKNOWN_CLIENT_VERSION;
        }
    };

        
    /**
     * Determines whether or not we need to look up the old row to retrieve old row values for maintaining the index.
     * @param m mutation being performed on the data table
     * @return true if prior row state is required and false otherwise
     */
    public boolean requiresPriorRowState(Mutation m);

    public ReplayWrite getReplayWrite();
    
    public int getClientVersion();
}
