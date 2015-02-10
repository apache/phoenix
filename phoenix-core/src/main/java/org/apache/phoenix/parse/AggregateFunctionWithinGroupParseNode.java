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

import java.util.List;

import org.apache.phoenix.compile.ColumnResolver;

public class AggregateFunctionWithinGroupParseNode extends AggregateFunctionParseNode {

    public AggregateFunctionWithinGroupParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }


    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append(' ');
        buf.append(getName());
        buf.append('(');
        List<ParseNode> children = getChildren();
        List<ParseNode> args = children.subList(2, children.size());
        if (!args.isEmpty()) {
            for (ParseNode child : args) {
                child.toSQL(resolver, buf);
                buf.append(',');
            }
            buf.setLength(buf.length()-1);
        }
        buf.append(')');
        
        buf.append(" WITHIN GROUP (ORDER BY ");
        children.get(0).toSQL(resolver, buf);
        buf.append(" " + (LiteralParseNode.TRUE.equals(children.get(1)) ? "ASC" : "DESC"));
        buf.append(')');
    }
}
