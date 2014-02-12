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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Interface for iterating through results returned from a scan, adding the
 * ability to peek at the next result.
 *
 * 
 * @since 0.1
 */
public interface PeekingResultIterator extends ResultIterator {
    public static final PeekingResultIterator EMPTY_ITERATOR = new PeekingResultIterator() {

        @Override
        public Tuple next() throws SQLException {
            return null;
        }

        @Override
        public Tuple peek() {
            return null;
        }

        @Override
        public void close() throws SQLException {
        }

        @Override
        public void explain(List<String> planSteps) {
        }
    };

    /**
     * Returns the next result without advancing the iterator
     * @throws SQLException
     */
    public Tuple peek() throws SQLException;
}
