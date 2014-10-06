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

public abstract class BooleanParseNodeVisitor<T> extends BaseParseNodeVisitor<T> {

    protected abstract boolean enterBooleanNode(ParseNode node) throws SQLException;
    protected abstract T leaveBooleanNode(ParseNode node, List<T> l) throws SQLException;
    protected abstract boolean enterNonBooleanNode(ParseNode node) throws SQLException;
    protected abstract T leaveNonBooleanNode(ParseNode node, List<T> l) throws SQLException;
    
    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(LikeParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(OrParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(FunctionParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(ComparisonParseNode node, List<T> l)
            throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(CaseParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(AddParseNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(AddParseNode node, List<T> l) throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(MultiplyParseNode node, List<T> l) throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(ModulusParseNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(ModulusParseNode node, List<T> l) throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(DivideParseNode node, List<T> l) throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(SubtractParseNode node, List<T> l) throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(NotParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(ExistsParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(ExistsParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(InListParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(InParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(InParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(IsNullParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(IsNullParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public T visit(ColumnParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(LiteralParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(BindParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(WildcardParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(TableWildcardParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(FamilyWildcardParseNode node) throws SQLException {
        return null;
    }

    @Override
    public T visit(SubqueryParseNode node) throws SQLException {
        return null;
    }

    @Override
    public boolean visitEnter(StringConcatParseNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(StringConcatParseNode node, List<T> l)
            throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(BetweenParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(BetweenParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(CastParseNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(CastParseNode node, List<T> l) throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(RowValueConstructorParseNode node)
            throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(RowValueConstructorParseNode node, List<T> l)
            throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(ArrayConstructorNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(ArrayConstructorNode node, List<T> l)
            throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

    @Override
    public T visit(SequenceValueParseNode node) throws SQLException {
        return null;
    }

    @Override
    public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(ArrayAllComparisonNode node, List<T> l)
            throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException {
        return enterBooleanNode(node);
    }

    @Override
    public T visitLeave(ArrayAnyComparisonNode node, List<T> l)
            throws SQLException {
        return leaveBooleanNode(node, l);
    }

    @Override
    public boolean visitEnter(ArrayElemRefNode node) throws SQLException {
        return enterNonBooleanNode(node);
    }

    @Override
    public T visitLeave(ArrayElemRefNode node, List<T> l) throws SQLException {
        return leaveNonBooleanNode(node, l);
    }

}

