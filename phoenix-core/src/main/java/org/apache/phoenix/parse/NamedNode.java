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

public class NamedNode {
    private final String name;
    private final boolean isCaseSensitive;
    
    public static NamedNode caseSensitiveNamedNode(String name) {
        return new NamedNode(name,true);
    }
    
    NamedNode(String name, boolean isCaseSensitive) {
        this.name = name;
        this.isCaseSensitive = isCaseSensitive;
    }

    NamedNode(String name) {
        this.name = SchemaUtil.normalizeIdentifier(name);
        this.isCaseSensitive = name == null ? false : SchemaUtil.isCaseSensitive(name);
    }

    public String getName() {
        return name;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
    
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NamedNode other = (NamedNode)obj;
        return name.equals(other.name);
    }
    
    @Override
    public String toString() {
        return (isCaseSensitive ? "\"" : "" ) + name + (isCaseSensitive ? "\"" : "" );
    }
}
