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

import java.util.Map;

import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Maps;

public class RuntimeContextImpl implements RuntimeContext {
    Map<String, VariableEntry> correlateVariables;

    public RuntimeContextImpl() {
        this.correlateVariables = Maps.newHashMap();
    }
    
    @Override
    public void defineCorrelateVariable(String variableId, TableRef def) {
        this.correlateVariables.put(variableId, new VariableEntry(def));
    }
    
    @Override
    public TableRef getCorrelateVariableDef(String variableId) {
        VariableEntry entry = this.correlateVariables.get(variableId);
        if (entry == null)
            throw new RuntimeException("Variable '" + variableId + "' undefined.");
        
        return entry.getDef();
    }
    
    @Override
    public void setCorrelateVariableValue(String variableId, Tuple value) {
        VariableEntry entry = this.correlateVariables.get(variableId);
        if (entry == null)
            throw new RuntimeException("Variable '" + variableId + "' undefined.");
        
        entry.setValue(value);
    }

    @Override
    public Tuple getCorrelateVariableValue(String variableId) {
        VariableEntry entry = this.correlateVariables.get(variableId);
        if (entry == null)
            throw new RuntimeException("Variable '" + variableId + "' undefined.");
        
        return entry.getValue();
    }
    
    private static class VariableEntry {
        private final TableRef def;
        private Tuple value;
        
        VariableEntry(TableRef def) {
            this.def = def;
        }
        
        TableRef getDef() {
            return def;
        }
        
        Tuple getValue() {
            return value;
        }
        
        void setValue(Tuple value) {
            this.value = value;
        }
    }
}
