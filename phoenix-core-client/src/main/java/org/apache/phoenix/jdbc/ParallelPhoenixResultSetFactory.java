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
package org.apache.phoenix.jdbc;

import java.sql.ResultSet;
import java.util.concurrent.CompletableFuture;

public class ParallelPhoenixResultSetFactory {

    public static final ParallelPhoenixResultSetFactory INSTANCE =
            new ParallelPhoenixResultSetFactory();
    public static final String PHOENIX_PARALLEL_RESULTSET_TYPE = "phoenix.parallel.resultSet.type";

    public enum ParallelPhoenixResultSetType {
        PARALLEL_PHOENIX_RESULT_SET("ParallelPhoenixResultSet"),
        PARALLEL_PHOENIX_NULL_COMPARING_RESULT_SET("ParallelPhoenixNullComparingResultSet");

        private String name;

        ParallelPhoenixResultSetType(String name) {
            this.name = name;
        }

        static ParallelPhoenixResultSetType fromName(String name) {
            if (name == null) {
                return PARALLEL_PHOENIX_RESULT_SET;
            }
            for (ParallelPhoenixResultSetType type : ParallelPhoenixResultSetType.values()) {
                if (name.equals(type.getName())) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown ParallelPhoenixResultSetType: " + name);
        }

        public String getName() {
            return this.name;
        }
    }

    private ParallelPhoenixResultSetFactory() {
    }

    public ResultSet getParallelResultSet(ParallelPhoenixContext context,
            CompletableFuture<ResultSet> resultSet1, CompletableFuture<ResultSet> resultSet2) {
        // We only have 2 types now, hence use simple comparison rather than reflection for
        // performance
        String resultSetProperty =
                context.getProperties().getProperty(PHOENIX_PARALLEL_RESULTSET_TYPE);
        ParallelPhoenixResultSetType type =
                ParallelPhoenixResultSetType.fromName(resultSetProperty);
        ResultSet rs;
        switch (type) {
        case PARALLEL_PHOENIX_RESULT_SET:
            rs = new ParallelPhoenixResultSet(context, resultSet1, resultSet2);
            break;
        case PARALLEL_PHOENIX_NULL_COMPARING_RESULT_SET:
            rs = new ParallelPhoenixNullComparingResultSet(context, resultSet1, resultSet2);
            break;
        default:
            throw new IllegalArgumentException("Unknown ParallelPhoenixResultSetType: " + type);
        }
        return rs;
    }
}
