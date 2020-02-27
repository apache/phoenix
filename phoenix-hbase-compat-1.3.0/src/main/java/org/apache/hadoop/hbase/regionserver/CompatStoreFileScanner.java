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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

// This has to be in org.apache.hadoop.hbase.regionserver due to the default
// visibility of some methods
public abstract class CompatStoreFileScanner extends StoreFileScanner {

    public CompatStoreFileScanner(Reader reader, HFileScanner hfs, boolean useMVCC, boolean hasMVCC,
            long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn) {
        super(reader, hfs, useMVCC, hasMVCC, readPt);
    }

}
