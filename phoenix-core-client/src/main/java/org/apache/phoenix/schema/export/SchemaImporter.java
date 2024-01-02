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

import org.apache.phoenix.schema.PTable;

import java.io.IOException;

/**
 * Interface for importing schemas stored externally from Phoenix into Phoenix by converting the
 * schema into a PTable
 */
public interface SchemaImporter {

    /**
     *
     * @param schema String form of an external schema. The expected format of the schema depends
     *               on the implementation of the class.
     * @return a Phoenix PTable
     */
    PTable getTableFromSchema(String schema) throws IOException;

}
