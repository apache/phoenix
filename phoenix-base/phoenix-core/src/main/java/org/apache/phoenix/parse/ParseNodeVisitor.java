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
import java.util.List;



/**
 * 
 * Visitor for ParseNode in the node tree. Uses composite
 * visitor pattern with enter/leave calls for any
 * compound expression node. Only supported SQL constructs
 * have visit methods.  Unsupported constructs fall through
 * to {@link #visitEnter(CompoundParseNode)} for
 * compound parse nodes and {@link #visit(ParseNode)}
 * for terminal parse nodes.
 * 
 * 
 * @since 0.1
 */
public interface ParseNodeVisitor<E> {
    public List<E> newElementList(int size);
    public void addElement(List<E> a, E element);
    
    public boolean visitEnter(LikeParseNode node) throws SQLException;
    public E visitLeave(LikeParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(AndParseNode node) throws SQLException;
    public E visitLeave(AndParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(OrParseNode node) throws SQLException;
    public E visitLeave(OrParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(FunctionParseNode node) throws SQLException;
    public E visitLeave(FunctionParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(ComparisonParseNode node) throws SQLException;
    public E visitLeave(ComparisonParseNode node, List<E> l) throws SQLException;

    public boolean visitEnter(CaseParseNode node) throws SQLException;
    public E visitLeave(CaseParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(CompoundParseNode node) throws SQLException;
    public E visitLeave(CompoundParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(AddParseNode node) throws SQLException;
    public E visitLeave(AddParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(MultiplyParseNode node) throws SQLException;
    public E visitLeave(MultiplyParseNode node, List<E> l) throws SQLException;

    public boolean visitEnter(ModulusParseNode node) throws SQLException;
    public E visitLeave(ModulusParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(DivideParseNode node) throws SQLException;
    public E visitLeave(DivideParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(SubtractParseNode node) throws SQLException;
    public E visitLeave(SubtractParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(NotParseNode node) throws SQLException;
    public E visitLeave(NotParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(ExistsParseNode node) throws SQLException;
    public E visitLeave(ExistsParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(InListParseNode node) throws SQLException;
    public E visitLeave(InListParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(InParseNode node) throws SQLException;
    public E visitLeave(InParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(IsNullParseNode node) throws SQLException;
    public E visitLeave(IsNullParseNode node, List<E> l) throws SQLException;
    
    public E visit(ColumnParseNode node) throws SQLException;
    public E visit(LiteralParseNode node) throws SQLException;
    public E visit(BindParseNode node) throws SQLException;
    public E visit(WildcardParseNode node) throws SQLException;
    public E visit(TableWildcardParseNode node) throws SQLException;
    public E visit(FamilyWildcardParseNode node) throws SQLException;
    public E visit(SubqueryParseNode node) throws SQLException;
    public E visit(ParseNode node) throws SQLException;  
    
    public boolean visitEnter(StringConcatParseNode node) throws SQLException;
    public E visitLeave(StringConcatParseNode node, List<E> l) throws SQLException;
	
    public boolean visitEnter(BetweenParseNode node) throws SQLException;
    public E visitLeave(BetweenParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(CastParseNode node) throws SQLException;
    public E visitLeave(CastParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(RowValueConstructorParseNode node) throws SQLException;
    public E visitLeave(RowValueConstructorParseNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(ArrayConstructorNode node) throws SQLException;
    public E visitLeave(ArrayConstructorNode node, List<E> l) throws SQLException;
    public E visit(SequenceValueParseNode node) throws SQLException;
    
    public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException;
    public E visitLeave(ArrayAllComparisonNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException;
    public E visitLeave(ArrayAnyComparisonNode node, List<E> l) throws SQLException;
    
    public boolean visitEnter(ArrayElemRefNode node) throws SQLException;
    public E visitLeave(ArrayElemRefNode node, List<E> l) throws SQLException;
    
    
}
