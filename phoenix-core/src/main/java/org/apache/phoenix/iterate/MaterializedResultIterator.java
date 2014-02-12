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
import java.util.*;

import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Fully materialized result iterator backed by the result list provided.
 * No copy is made of the backing results collection.
 *
 * 
 * @since 0.1
 */
public class MaterializedResultIterator implements PeekingResultIterator {
    private final PeekingCollectionIterator iterator;
    
    public MaterializedResultIterator(Collection<Tuple> results) {
        iterator = new PeekingCollectionIterator(results);
    }
    
    @Override
    public void close() {
    }

    @Override
    public Tuple next() throws SQLException {
        return iterator.nextOrNull();
    }

    @Override
    public Tuple peek() throws SQLException {
        return iterator.peek();
    }

    private static class PeekingCollectionIterator implements Iterator<Tuple> {
        private final Iterator<Tuple> iterator;
        private Tuple current;            
        
        private PeekingCollectionIterator(Collection<Tuple> results) {
            iterator = results.iterator();
            advance();
        }
        
        private Tuple advance() {
            if (iterator.hasNext()) {
                current = iterator.next();
            } else {
                current = null;
            }
            return current;
        }
        
        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Tuple next() {
            Tuple next = nextOrNull();
            if (next == null) {
                throw new NoSuchElementException();
            }
            return next;
        }

        public Tuple nextOrNull() {
            if (current == null) {
                return null;
            }
            Tuple next = current;
            advance();
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
        
        public Tuple peek() {
            return current;
        }

    }

    @Override
    public void explain(List<String> planSteps) {
    }
}
