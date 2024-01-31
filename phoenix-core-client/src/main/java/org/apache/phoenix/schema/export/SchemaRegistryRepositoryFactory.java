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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class SchemaRegistryRepositoryFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistryRepository.class);
    private static SchemaRegistryRepository exporter;

    public synchronized static SchemaRegistryRepository getSchemaRegistryRepository(Configuration conf)
            throws IOException {
        if (exporter != null) {
            return exporter;
        }
        try {
            String className = conf.get(SchemaRegistryRepository.SCHEMA_REGISTRY_IMPL_KEY);
            if (className == null) {
                exporter = new DefaultSchemaRegistryRepository();
            } else {
                Class<SchemaRegistryRepository> clazz =
                    (Class<SchemaRegistryRepository>) Class.forName(className);
                exporter = clazz.newInstance();
            }
            exporter.init(conf);
            return exporter;
        } catch (Exception e) {
            LOGGER.error("Error constructing SchemaRegistryExporter object", e);
            if (exporter != null) {
                try {
                    exporter.close();
                    exporter = null;
                } catch (IOException innerE) {
                    LOGGER.error("Error closing incorrectly constructed SchemaRegistryExporter", e);
                }
            }
            throw new IOException(e);
        }
    }

    public synchronized static void close() throws IOException {
        if (exporter != null) {
            exporter.close();
            exporter = null;
        }
    }
}
