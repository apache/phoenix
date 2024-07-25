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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;

public class CompatIndexHalfStoreFileReader extends StoreFileReader {

    public CompatIndexHalfStoreFileReader(final FileSystem fs, final CacheConfig cacheConf,
                                          final Configuration conf,
                                          final ReaderContext readerContext,
                                          final HFileInfo hFileInfo, Path p) throws IOException {
        super(readerContext, hFileInfo, cacheConf, new AtomicInteger(0), conf);
    }


}
