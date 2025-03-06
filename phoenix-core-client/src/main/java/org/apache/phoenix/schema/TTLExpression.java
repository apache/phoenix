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
package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.Map;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.CreateTableStatement;

public interface TTLExpression {

    /**
     * Returns the representation of the ttl expression as specified in the DDL
     * @return string representation
     */
    String getTTLExpression();

    String toString();

    /**
     * Validate the TTL expression on CREATE [TABLE | VIEW | INDEX]
     * @param conn Phoenix connection
     * @param create CreateTable statement
     * @param parent Null for base tables, parent of view or index
     * @param tableProps Table properties passed in CREATE statement
     * @throws SQLException
     */
    void validateTTLOnCreate(PhoenixConnection conn,
                             CreateTableStatement create,
                             PTable parent,
                             Map<String, Object> tableProps) throws SQLException;

    /**
     * Validate the TTL expression on ALTER [TABLE | VIEW]
     * @param connection Phoenix connection
     * @param table PTable of the entity being changed
     * @throws SQLException
     */
    void validateTTLOnAlter(PhoenixConnection connection,
                            PTable table) throws SQLException;

    /**
     * Compile the TTL expression so that it can be evaluated against of a row of cells
     * @param connection Phoenix connection
     * @param table PTable
     * @return CompiledTTLExpression object
     * @throws SQLException
     */
    CompiledTTLExpression compileTTLExpression(PhoenixConnection connection,
                                       PTable table) throws SQLException;

}
