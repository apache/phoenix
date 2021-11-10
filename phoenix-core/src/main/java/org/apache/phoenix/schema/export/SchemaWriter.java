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

package org.apache.phoenix.schema.export;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.PTable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for classes to implement for converting Phoenix schema data, captured in a PTable, into
 * some other format interpretable by an external system. Possible implementing classes would include
 * converters to Avro, Protobuf, Thrift, various dialects of SQL,
 * or other similar cross-platform data description languages.
 */
public interface SchemaWriter extends Closeable {
    public static final String SCHEMA_REGISTRY_IMPL_KEY =
            "org.apache.phoenix.export.schemawriter.impl";

    /**
     * Initialize the schema writer with appropriate configuration
     * @param conf a Configuration object
     * @throws IOException if something goes wrong during initialization
     */
    void init(Configuration conf) throws IOException;

    /**
     * Given a Phoenix PTable, output a schema document readable by some external system.
     * @param table A Phoenix PTable describing a table or view
     * @return a String interpretable as a data format schema in an external system
     */
    String exportSchema(PTable table) throws IOException;

}
