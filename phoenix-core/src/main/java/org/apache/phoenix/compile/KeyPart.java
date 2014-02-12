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
package org.apache.phoenix.compile;

import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;

/**
 * 
 * Interface that determines how a key part contributes to
 * the forming of the key (start/stop of scan and SkipScanFilter)
 * for each part of a multi-part primary key. It acts as the glue
 * between a built-in function and the setting of the scan key
 * during query compilation.
 * 
 * 
 * @since 0.12
 */
public interface KeyPart {
    /**
     * Calculate the key range given an operator and the key on
     * the RHS of an expression. For example, given the expression
     * SUBSTR(foo,1,3) = 'bar', the key range would be ['bar','bas'),
     * and if foo was fixed length, the upper and lower key range
     * bytes would be filled out to the fixed length.
     * @param op comparison operator (=, <=, <, >=, >, !=)
     * @param rhs the constant on the RHS of an expression.
     * @return the key range that encompasses the range for the
     *  expression for which this keyPart is associated
     *  
     * @see org.apache.phoenix.expression.function.ScalarFunction#newKeyPart(KeyPart)
     */
    public KeyRange getKeyRange(CompareOp op, Expression rhs);
    
    /**
     * Determines whether an expression gets extracted from the
     * WHERE clause if it contributes toward the building of the
     * scan key. For example, the SUBSTR built-in function may
     * be extracted, since it may be completely represented
     * through a key range. However, the REGEXP_SUBSTR must be
     * left in the WHERE clause, since only the constant prefix
     * part of the evaluation can be represented through a key
     * range (i.e. rows may pass through that will fail when
     * the REGEXP_SUBSTR is evaluated).
     * 
     * @return an empty list if the expression should remain in
     * the WHEERE clause for post filtering or a singleton list
     * containing the expression if it should be removed.
     */
    public List<Expression> getExtractNodes();
    
    /**
     * Gets the primary key column associated with the start of this key part.
     * @return the primary key column for this key part
     */
    public PColumn getColumn();
}