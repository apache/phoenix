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

import java.sql.SQLException;


/**
 * 
 * Visitor that traverses into all parse nodes
 *
 * 
 * @since 0.1
 */
public abstract class TraverseAllParseNodeVisitor<T> extends BaseParseNodeVisitor<T> {
    protected abstract void enterParseNode(ParseNode node) throws SQLException;
    
    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(ExistsParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(CastParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(InParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(IsNullParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(AddParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(ModulusParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }

    @Override
    public boolean visitEnter(BetweenParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public T visit(ColumnParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }
    
    @Override
    public T visit(LiteralParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }
    
    @Override
    public T visit(BindParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }

    @Override
    public T visit(WildcardParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }

    @Override
    public T visit(TableWildcardParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }

    @Override
    public T visit(FamilyWildcardParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }

    @Override
    public T visit(SubqueryParseNode node) throws SQLException {
        enterParseNode(node);
        return null;
    }
    
    @Override
    public boolean visitEnter(StringConcatParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(RowValueConstructorParseNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public T visit(SequenceValueParseNode node) throws SQLException {			
        enterParseNode(node);
		return null;
	}
    
    @Override
    public boolean visitEnter(ArrayConstructorNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
    
    @Override
    public boolean visitEnter(ArrayElemRefNode node) throws SQLException {
        enterParseNode(node);
        return true;
    }
}
