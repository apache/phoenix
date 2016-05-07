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
package org.apache.phoenix.execute;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;

public interface RuntimeContext {
    ThreadLocal<List<RuntimeContext>> THREAD_LOCAL =
            new ThreadLocal<List<RuntimeContext>>() {
        @Override protected List<RuntimeContext> initialValue() {
            return new LinkedList<RuntimeContext>();
        }
    };
    
    public interface CorrelateVariable {
        public Expression newExpression(int index);        
        public Tuple getValue();        
        public void setValue(Tuple value);
    }

    public void defineCorrelateVariable(String variableId, CorrelateVariable def);
    public CorrelateVariable getCorrelateVariable(String variableId);
    
    public void setBindParameterValues(Map<String, Object> values);
    public Object getBindParameterValue(String name);
}