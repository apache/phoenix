/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema.export;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.PTable;

/**
 * Interface for exporting a Phoenix object (e.g a table or view) to an external schema repository
 * The choice of which schema repository, and the transport mechanism, are deferred to implementing
 * classes
 */
public interface SchemaRegistryRepository extends Closeable {

  String SCHEMA_WRITER_IMPL_KEY = "org.apache.phoenix.export.schemawriter.impl";
  String SCHEMA_REGISTRY_IMPL_KEY = "org.apache.phoenix.export.schemaregistry.impl";

  /**
   * Optional method for any necessary bootstrapping to connect to the external schema registry
   * @param conf Configuration object with necessary parameters to talk to the external schema
   *             registry
   * @throws IOException Exception if something goes wrong in connecting to the registry
   */
  void init(Configuration conf) throws IOException;

  /**
   * Export a Phoenix PTable into an external schema registry by reformatting it into a suitable
   * form.
   * @param writer An object which can translate a PTable into a String suitable for the external
   *               schema registry
   * @param table  a Phoenix PTable for a table or view
   * @return Schema id generated by the schema registry, represented as a string.
   * @throws IOException Exception if something goes wrong in constructing or sending the schema
   */
  String exportSchema(SchemaWriter writer, PTable table) throws IOException;

  /**
   * Return a schema from an external schema repository by its unique identifier
   * @param schemaId schema identifier
   * @return a schema
   */
  String getSchemaById(String schemaId) throws IOException;

  /**
   * Return a schema from an external schema repository using information on a PTable
   * @param table a Phoenix PTable for a table or view
   * @return a schema
   */
  String getSchemaByTable(PTable table) throws IOException;
}
