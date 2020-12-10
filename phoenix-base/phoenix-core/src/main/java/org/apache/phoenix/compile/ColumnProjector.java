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

import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;



/**
 * 
 * Interface used to access the value of a projected column.
 * 
 * 
 * @since 0.1
 */
public interface ColumnProjector {
    /**
     * Get the column name as it was referenced in the query
     * @return the database column name
     */
    String getName();
    
    /**
     * Get the expression
     * @return the expression for the column projector
     */
    public Expression getExpression();
    
    // TODO: An expression may contain references to multiple tables.
    /**
     * Get the name of the hbase table containing the column
     * @return the hbase table name
     */
    String getTableName();
    
    /**
     * Get the value of the column, coercing it if necessary to the specified type
     * @param tuple the row containing the column
     * @param type the type to which to coerce the binary value
     * @param ptr used to retrieve the value
     * @return the object representation of the column value.
     * @throws SQLException
     */
    Object getValue(Tuple tuple, PDataType type, ImmutableBytesWritable ptr) throws SQLException;
    
    boolean isCaseSensitive();
}
