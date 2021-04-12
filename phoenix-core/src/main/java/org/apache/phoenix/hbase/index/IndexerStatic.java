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
package org.apache.phoenix.hbase.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.query.QueryConstants;

public class IndexerStatic {
    /** Configuration key for the {@link IndexBuilder} to use */
    public static final String INDEX_BUILDER_CONF_KEY = "index.builder";
    
    /**
     * Enable indexing on the given table
     * @param descBuilder {@link TableDescriptor} for the table on which indexing should be enabled
     * @param builder class to use when building the index for this table
     * @param properties map of custom configuration options to make available to your
     *          {@link IndexBuilder} on the server-side
     * @param priority TODO
     * @throws IOException the Indexer coprocessor cannot be added
     */
    public static void enableIndexing(TableDescriptorBuilder descBuilder, String IndexBuilderClassName,
        Map<String, String> properties, int priority) throws IOException {
      if (properties == null) {
        properties = new HashMap<String, String>();
      }
      properties.put(IndexerStatic.INDEX_BUILDER_CONF_KEY, IndexBuilderClassName);
       descBuilder.addCoprocessor(QueryConstants.INDEXER_CLASSNAME, null, priority, properties);
    }
}
