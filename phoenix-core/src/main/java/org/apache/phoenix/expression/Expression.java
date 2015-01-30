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
package org.apache.phoenix.expression;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Interface for general expression evaluation
 *
 * 
 * @since 0.1
 */
public interface Expression extends PDatum, Writable {
	
    /**
     * Access the value by setting a pointer to it (as opposed to making
     * a copy of it which can be expensive)
     * @param tuple Single row result during scan iteration
     * @param ptr Pointer to byte value being accessed
     * @return true if the expression could be evaluated (i.e. ptr was set)
     * and false otherwise
     */
    boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr);
    
    /**
     * Means of traversing expression tree through visitor.
     * @param visitor
     */
    <T> T accept(ExpressionVisitor<T> visitor);
    
    /**
     * @return the child expressions
     */
    List<Expression> getChildren();
    
    /**
     * Resets the state of a expression back to its initial state and
     * enables the expession to be evaluated incrementally (which
     * occurs during filter evaluation where we see one key value at
     * a time; it's possible to evaluate immediately rather than
     * wait until all key values have been seen). Note that when
     * evaluating incrementally, you must call this method before
     * processing a new row.
     */
    void reset();
    
    /**
     * @return true if the expression can be evaluated on the client
     * side with out server state. If a sequence is involved, you
     * still need a Tuple from a {@link org.apache.phoenix.iterate.SequenceResultIterator},
     * but otherwise the Tuple may be null.
     */
    boolean isStateless();
    
    /**
     * @return Determinism enum 
     */
    Determinism getDeterminism();
    
    /**
     * Determines if an evaluate is required after partial evaluation
     * is run. For example, in the case of an IS NULL expression, we
     * only know we can return TRUE after all KeyValues have been seen
     * while an expression is used in the context of a filter.
     * @return
     */
    boolean requiresFinalEvaluation();
}
