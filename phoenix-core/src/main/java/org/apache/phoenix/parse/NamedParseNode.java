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



/**
 * 
 * Abstract node representing named nodes such as binds and column expressions in SQL
 *
 * 
 * @since 0.1
 */
public abstract class NamedParseNode extends TerminalParseNode{
    private final NamedNode namedNode;
    
    NamedParseNode(NamedParseNode node) {
        this.namedNode = node.namedNode;
    }

    NamedParseNode(String name) {
        this.namedNode = new NamedNode(name);
    }
    
    NamedParseNode(String name, boolean isCaseSensitive) {
        this.namedNode = new NamedNode(name, isCaseSensitive);
    }

    public String getName() {
        return namedNode.getName();
    }

    public boolean isCaseSensitive() {
        return namedNode.isCaseSensitive();
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((namedNode == null) ? 0 : namedNode.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NamedParseNode other = (NamedParseNode) obj;
		if (namedNode == null) {
			if (other.namedNode != null)
				return false;
		} else if (!namedNode.equals(other.namedNode))
			return false;
		return true;
	}

    
    public void toSQL(StringBuilder buf) {
        if (isCaseSensitive()) {
            buf.append('"');
            buf.append(getName());
            buf.append('"');
        } else {
            buf.append(getName());
        }
    }
}
