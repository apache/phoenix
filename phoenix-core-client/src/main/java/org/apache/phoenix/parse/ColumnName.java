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
package org.apache.phoenix.parse;

import org.apache.phoenix.util.SchemaUtil;


public class ColumnName {
    private final NamedNode familyNode;
    private final NamedNode columnNode;
    
    public static ColumnName caseSensitiveColumnName(String familyName, String columnName) {
        return new ColumnName(NamedNode.caseSensitiveNamedNode(familyName), NamedNode.caseSensitiveNamedNode(columnName));
    }
    
    public static ColumnName caseSensitiveColumnName(String columnName) {
        return new ColumnName(null, NamedNode.caseSensitiveNamedNode(columnName));
    }
    
    public static ColumnName newColumnName(NamedNode columnName) {
        return new ColumnName(null, columnName);
    }
    
    public static ColumnName newColumnName(NamedNode familyName, NamedNode columnName) {
        return new ColumnName(familyName, columnName);
    }
    
    private ColumnName(NamedNode familyNode, NamedNode columnNode) {
        this.familyNode = familyNode;
        this.columnNode = columnNode;
    }
    

    ColumnName(String familyName, String columnName) {
        this.familyNode = familyName == null ? null : new NamedNode(familyName);
        this.columnNode = new NamedNode(columnName);
    }

    ColumnName(String columnName) {
        this(null, columnName);
    }

    public String getFamilyName() {
        return familyNode == null ? null : familyNode.getName();
    }

    public String getColumnName() {
        return columnNode.getName();
    }

    public NamedNode getFamilyNode() {
        return familyNode;
    }

    public NamedNode getColumnNode() {
        return columnNode;
    }

    @Override
    public String toString() {
		return SchemaUtil.getColumnName(getFamilyName(),getColumnName());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columnNode.hashCode();
        result = prime * result + ((familyNode == null) ? 0 : familyNode.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnName other = (ColumnName)obj;
        if (!columnNode.equals(other.columnNode)) return false;
        if (familyNode == null) {
            if (other.familyNode != null) return false;
        } else if (!familyNode.equals(other.familyNode)) return false;
        return true;
    }
}
