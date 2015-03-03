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

public class ProjectedColumn extends DelegateColumn {
    
    private final PName name;
    private final PName familyName;
    private final int position;
    private final boolean nullable;
    private final ColumnRef sourceColumnRef;

    public ProjectedColumn(PName name, PName familyName, int position, boolean nullable, ColumnRef sourceColumnRef) {
        super(sourceColumnRef.getColumn());
        this.name = name;
        this.familyName = familyName;
        this.position = position;
        this.nullable = nullable;
        this.sourceColumnRef = sourceColumnRef;
    }
    
    @Override
    public PName getName() {
        return name;
    }
    
    public PName getFamilyName() {
        return familyName;
    }
    
    @Override
    public int getPosition() {
        return position;
    }
    
    @Override
    public boolean isNullable() {
        return nullable;
    }

    public ColumnRef getSourceColumnRef() {
        return sourceColumnRef;
    }
}
